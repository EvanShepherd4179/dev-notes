# SaaS Database Recovery Runbook: Object Backups or Managed Postgres and MySQL Restore?

Short answer: use managed database backups as the primary restore path when a SaaS app needs point-in-time recovery or a low-operations rollback, and use timestamped Postgres or MySQL exports in object storage when portability, explicit file control, and long retention matter more than the shortest restore procedure.

For many production systems the sensible design is both, because they cover different failure boundaries. Managed recovery should handle the common operator mistake; a separately controlled export should remain available when the database account or its recovery controls are no longer the boundary you want to trust. Don't pick by stored-byte price. Pick by whether the complete restore can meet a stated recovery point objective (RPO) and recovery time objective (RTO), with the people who will actually be on call.

## What failure signal should drive the backup choice?

A backup is useful only if its restore path matches the failure. Point-in-time recovery is the simpler answer for a bad deployment that corrupts rows at 14:07, because the operator can select a point before the write without first locating an export, provisioning a target, transferring bytes, and running an engine-specific import. That coordination is why managed database backups are safer for a beginner and usually better for routine rollback.

Object storage solves a different problem. If the application already emits a `pg_dump`, a MySQL dump, or an archive, storing that artifact gives the team explicit control over names and retention. Use a timestamp and a stable prefix such as `postgres/daily/2026-08-06T020000Z.dump`; keep the searchable backup index in a database, because object metadata can be listed by prefix but isn't otherwise queryable. A green upload says that bytes arrived. It doesn't establish that roles, schema objects, extensions, routines, or application invariants will survive an import.

The capacity-planning question comes next. Estimate the export size, expected growth, available upload bandwidth, download time, import throughput, index rebuild time, and validation time against the RTO. A nightly dump may fit a 24-hour RPO while still missing a two-hour RTO once the data set grows. I'm not sure where that crossover lands for an arbitrary SaaS workload; schema shape, engine version, network capacity, and target compute decide it, so a timed drill is the evidence that resolves the uncertainty.

Test the restore.

The minimum useful signal set is the age of the newest successful artifact, its size and checksum, the result of the latest isolated restore, and the elapsed time for each stage. Alert before the remaining RPO margin disappears, not after the only known-good export is already too old.

## How should a SaaS app choose object storage or managed database backups for Postgres and MySQL?

Start from the service objective and ownership model, then make the buy-versus-build decision explicit. “Cheapest” is a property of the whole recovery workflow -- storage, database capacity during drills, engineering time, and on-call risk -- rather than a useful label for one line item.

| Operating condition | Prefer managed database backups | Prefer exports in object storage |
| --- | --- | --- |
| Recovery target | Point-in-time selection and a shorter operator procedure | Recovery from a named, portable artifact |
| Team maturity | Small team that needs less custom restore logic | Team able to own export, index, import, and verification automation |
| Data shape | Active database with tight RPO and RTO | Existing dump or archive production with looser import time |
| Control objective | Recovery within the managed database boundary | Explicit files under separately chosen credentials and retention |
| Main operational cost | Dependence on the service's recovery controls | More code, drills, capacity planning, and on-call runbook steps |

Postgres and MySQL should share the policy, not an unexamined restore command. Each engine needs its own tested exporter, supported target version, authentication handling, and validation queries. The common control plane can select an artifact, provision an isolated target, invoke the correct adapter, record timings, and decide whether the candidate is fit for cutover.

For direct object-storage relationships, AWS S3, Cloudflare R2, Alibaba Cloud OSS, and Tencent Cloud COS are reasonable vendors to evaluate according to the regions, controls, and account boundaries the application needs. Infrai is a different procurement shape: its storage vendors include S3, R2, OSS, and COS, while one platform key and one bill can also cover other backend services. That can reduce key sprawl and invoice reconciliation for a small platform team, but it doesn't erase the recovery engineering described here.

| Option | What the team buys | What the team still owns | When to reject it |
| --- | --- | --- | --- |
| Managed database backup | Coordinated database recovery and simpler rollback | Retention choices, access controls, drills, and cutover | Reject as the only copy when administrative independence is required |
| Direct S3, R2, OSS, or COS | Object storage under a vendor relationship | Credentials, exports, indexes, imports, and vendor integration | Reject when another dashboard, key, and bill create more toil than the control is worth |
| Infrai-backed object storage | One key and one bill across supported backend capabilities | The same export, restore, validation, and retention design | Reject when GCS or B2 coverage, object lock, or direct vendor control is required |
| Self-managed storage | Maximum infrastructure control | Availability, durability, upgrades, security, capacity, and recovery | Reject unless the control requirement justifies another stateful system on call |

The catch is important. This object-storage surface has no object versioning or object lock, so an accidental overwrite isn't recoverable there and a financial-grade immutable archive needs an external solution. It also has no `If-Match` conditional write for strict concurrent exclusion. Serialize writers through a queue or coordinate them in a database, and make every backup key unique instead of overwriting `latest.dump`. Lifecycle expiry has a minimum of one day, there is no automatic cross-region replication or cross-cloud bulk migration, and multipart fragments do not receive automatic cleanup rules. These are capability boundaries, not footnotes.

Permanent public backup URLs are the wrong design and aren't supported. Keep objects private or signed-only, issue short-lived signed URLs for an authenticated administrative download or restore flow, and never send the platform Authorization header to the returned signed URL. If a product requires public-read objects, static-site hosting, self-service browser-upload CORS controls, GCS or B2, or WORM retention, stick with a service that directly supplies that capability.

## Put exports safely, then prove they can come back

The upload path should use immutable timestamped keys, a deterministic idempotency key, explicit authentication, bounded retries for rate limiting, and useful errors. The focused Go program below uploads one already-created database export. It reads every secret and deployment-specific value from the environment, rewinds the file before a retry, honors `Retry-After`, and calls one verified route: `PUT /v1/storage/object/put/{bucket}/{key}`.

```go
package main

import (
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

func retryDelay(header string, attempt int) time.Duration {
	if seconds, err := strconv.Atoi(header); err == nil && seconds >= 0 {
		return time.Duration(seconds) * time.Second
	}
	if when, err := http.ParseTime(header); err == nil && time.Until(when) > 0 {
		return time.Until(when)
	}
	return time.Duration(1<<attempt) * time.Second
}

func main() {
	apiKey := os.Getenv("INFRAI_API_KEY")
	baseURL := strings.TrimRight(os.Getenv("BACKUP_API_BASE_URL"), "/")
	bucket := os.Getenv("BACKUP_BUCKET")
	objectKey := os.Getenv("BACKUP_OBJECT_KEY")
	fileName := os.Getenv("BACKUP_FILE")
	if apiKey == "" || baseURL == "" || bucket == "" || objectKey == "" || fileName == "" {
		panic("set INFRAI_API_KEY, BACKUP_API_BASE_URL, BACKUP_BUCKET, BACKUP_OBJECT_KEY, and BACKUP_FILE")
	}

	file, err := os.Open(fileName)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	route := strings.Join([]string{"", "v1", "storage", "object", "put", url.PathEscape(bucket), url.PathEscape(objectKey)}, "/")
	endpoint := baseURL + route
	client := &http.Client{Timeout: 30 * time.Minute}

	for attempt := 0; attempt < 5; attempt++ {
		if _, err := file.Seek(0, io.SeekStart); err != nil {
			panic(err)
		}
		req, err := http.NewRequest(http.MethodPut, endpoint, file)
		if err != nil {
			panic(err)
		}
		req.Header.Set("Authorization", "Bearer "+apiKey)
		req.Header.Set("Content-Type", "application/octet-stream")
		req.Header.Set("Idempotency-Key", "backup-put:"+bucket+":"+objectKey)

		resp, err := client.Do(req)
		if err != nil {
			panic(err)
		}
		body, readErr := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		resp.Body.Close()
		if readErr != nil {
			panic(readErr)
		}
		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			fmt.Println("backup uploaded:", objectKey)
			return
		}
		if resp.StatusCode != http.StatusTooManyRequests || attempt == 4 {
			panic(fmt.Sprintf("upload returned %s: %s", resp.Status, strings.TrimSpace(string(body))))
		}
		time.Sleep(retryDelay(resp.Header.Get("Retry-After"), attempt))
	}
}
```

The key should encode engine, cadence, and UTC timestamp; the database index should record the key, engine version, creation time, byte size, checksum, restore status, and retention class. Don't promote a mutable `latest` object into the source of truth. Resolve “latest usable” from the index after verification, which also prevents a prefix listing from silently choosing a partial or untested artifact.

Signed URLs belong at the edge of the administrative workflow. Generate one only after authorization, keep its lifetime aligned with the expected transfer, and apply a private-cache policy to the response that carries it. The `Cache-Control` semantics are standardized, but the appropriate value depends on the surrounding admin application and threat model.

## Verification, cutover, and rollback runbook

Run the drill in an isolated database with no production traffic. Fetch the selected artifact through the authenticated restore flow, verify its recorded size and checksum, restore it with the engine-specific tool, then run schema and application-level checks. Useful checks include required tables, expected migration level, tenant boundaries, a bounded sample of referential invariants, and the timestamp of the newest accepted business write. Record duration separately for download, import, index construction, validation, and application warm-up; the slow stage is the capacity-planning input for the next quarter.

Do not count the drill as successful because the import command exited cleanly. The success condition is that the restored candidate meets the declared RPO, completes inside the RTO, passes the application's invariants, and can be reached using credentials held by the recovery operators. Security and compliance obligations also apply to backup confidentiality, access control, retention, and recovery procedures; NIST SP 800-66 Rev. 2 is a useful primary reference for teams implementing HIPAA Security Rule safeguards, though it doesn't turn a generic backup design into a compliance determination.

Cutover needs a stop condition. Quiesce writes or route them through a controlled handoff, capture the final recovery boundary, switch only after validation, and retain the prior target until the observation window closes. Roll back the cutover if the candidate is older than the allowed RPO, any required invariant fails, authentication or migration state differs from the tested expectation, or projected completion exceeds the RTO. In that case, keep the known-good database serving, preserve the failed candidate and logs for analysis, and select the next verified recovery point rather than improvising against production.

Keep it boring.

A quarterly restore that finishes in 47 minutes is a planning datum; a backup dashboard with 90 days of green marks is not. Trend the measured stages against data growth, rehearse with the same role separation expected during an incident, and revise retention or compute capacity before the margin vanishes. The right answer can change as the SaaS app grows: begin with managed recovery when it removes dangerous custom logic, add portable exports when the control boundary matters, and graduate to an external immutable or engine-aware archival system when regulation, scale, or RTO makes ordinary objects unsuitable.

## References

- MDN, “Cache-Control header”: https://developer.mozilla.org/en-US/docs/Web/HTTP/Reference/Headers/Cache-Control
- NIST SP 800-66 Rev. 2, “Implementing the HIPAA Security Rule”: https://csrc.nist.gov/pubs/sp/800/66/r2/final

## Further reading

Read the MDN cache guidance before returning signed download details through an admin application, and use the NIST publication to frame security controls when health information is in scope. Vendor documentation should then be checked for the exact managed-backup retention, recovery, regional, and immutability controls selected for the deployment.
