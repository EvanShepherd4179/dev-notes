# Object Storage Bucket Prefixes for Full App Backup of User Uploads

Short answer: a nightly backup should publish one recoverable application snapshot, not two unrelated successful transfers. Put the database export and the user-upload inventory under one timestamped object-storage prefix, write a manifest last, and make a timed restore drill the acceptance test. This meets a daily RPO only when its measured restore duration fits the service RTO.

The dangerous version of this work looks ordinary: a Node.js scheduler finishes, the bucket has more bytes than yesterday, and the database export or upload set is still unusable at the moment it is needed. A successful write says little about recoverability. A completed snapshot is a contract with a future operator.

## Should a full app backup keep user uploads plus the database dump together?

The application couples them. Database rows refer to file keys; a later delete can change that relationship; a file created between the database export and a directory walk may have no corresponding row in the recovered state. The unit of recovery therefore needs a declared boundary.

Use a prefix shaped around that boundary, with an RFC 3339 UTC timestamp that sorts in chronological order:

```text
services/billing/production/2026-08-07T02:15:00Z/database/export.dump
services/billing/production/2026-08-07T02:15:00Z/files/9c/9c3f1a.bin
services/billing/production/2026-08-07T02:15:00Z/manifest.json
```

The final object is the important one. `manifest.json` records each key, its byte count, checksum, database format, and the snapshot identifier. A restore tool accepts a prefix only when the manifest exists and every entry validates. Prefixes without a manifest are incomplete work, not restore candidates.

Do not split `database/` and `files/` into unrelated date roots merely because the bucket browser looks tidier. That turns recovery into timestamp reconciliation under pressure. One snapshot root also lets retention rules and access policy follow the thing that will actually be restored.

## How should a Node.js nightly job publish a snapshot safely?

The job needs a hard boundary between collection and publication. First establish database consistency using a method appropriate to its write rate: a short quiesce window can be acceptable for a small application, while a busier database may need its native backup and recovery mechanism. Then export the database, enumerate the exact upload set, upload the parts, validate them, and publish the manifest only after every required step is complete.

There are two reasonable transfer designs. A temporary local database-export file makes its size and checksum available before upload, while a stream lowers local disk demand but makes retry and end-to-end validation more complex. Capacity planning must include the largest expected dump, temporary-disk headroom, upload concurrency, and the recovery environment. The backup runner is a production workload with an SLO, not an administrative afterthought.

Several failures belong in this same job contract: a child database-export process can exit unsuccessfully while the scheduler reports success; a short object can be written; database roles, extensions, or restore-tool versions can differ from the recovery environment; and lifecycle policy can expire a component before its snapshot. Object count and stored bytes are weak signals for all of these. Require manifest publication, expected entry count, checksum or read-back validation, and snapshot age before calling a run successful. Alert before the RPO is breached, rather than after a human discovers that the newest usable snapshot is old.

The storage operation itself can remain small and vendor-independent. This Go example writes all parts beneath a prefix and commits the manifest last; database consistency is intentionally decided before this function is called.

```go
package backup

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
)

type Store interface {
	Put(context.Context, string, io.Reader) error
	Size(context.Context, string) (int64, error)
}

type Part struct {
	Name string
	Open func() (io.ReadCloser, error)
}

type Entry struct {
	Key string `json:"key"`
	Bytes int64 `json:"bytes"`
	SHA256 string `json:"sha256"`
}

func Commit(ctx context.Context, store Store, prefix string, parts []Part) ([]Entry, error) {
	entries := make([]Entry, 0, len(parts))
	for _, part := range parts {
		body, err := part.Open()
		if err != nil { return nil, fmt.Errorf("open %s: %w", part.Name, err) }
		hash, count := sha256.New(), &counter{}
		key := prefix + "/" + part.Name
		err = store.Put(ctx, key, io.TeeReader(body, io.MultiWriter(hash, count)))
		closeErr := body.Close()
		if err != nil { return nil, fmt.Errorf("write %s: %w", key, err) }
		if closeErr != nil { return nil, fmt.Errorf("close %s: %w", key, closeErr) }
		actual, err := store.Size(ctx, key)
		if err != nil || actual != count.n { return nil, fmt.Errorf("verify %s", key) }
		entries = append(entries, Entry{key, actual, hex.EncodeToString(hash.Sum(nil))})
	}
	doc, err := json.Marshal(entries)
	if err != nil { return nil, err }
	if err := store.Put(ctx, prefix+"/manifest.json", bytes.NewReader(doc)); err != nil { return nil, err }
	return entries, nil
}

type counter struct{ n int64 }
func (c *counter) Write(p []byte) (int, error) { c.n += int64(len(p)); return len(p), nil }
```

## What does a restore checklist for an app backup include?

Run this from an isolated recovery environment with read access to the selected snapshot and no ability to modify production:

1. Choose the newest prefix with a manifest, then validate the size and checksum of every recorded object.
2. Confirm the export format and restore-tool version; restore the database into an empty target.
3. Materialize the exact upload objects or immutable versions named by the manifest.
4. Start a scratch application against those two restored components and exercise records that read uploaded files.
5. Record wall-clock duration, snapshot timestamp, operator actions, and exceptions against the RTO and RPO.

The drill exposes the numbers that a storage price page cannot settle: index rebuild time, object-retrieval throughput, temporary capacity, and operator count. An RTO that has not survived a drill is an aspiration.

## When should a team choose a different recovery design?

The catch is that a nightly bundle is coarse by construction. It is not suitable when the RPO is measured in minutes; use continuous database archiving and point-in-time recovery for that database requirement, while designing separate protection for uploaded objects. It also stops fitting when logical-export windows or measured restore time exceed the RTO.

| Recovery design | Primary operational burden | Appropriate boundary |
| --- | --- | --- |
| Nightly export and manifest | Scheduler, validation, restore drills | Daily RPO and modest recovery targets |
| Continuous database archiving | WAL/base-backup retention and monitoring | Low-RPO database recovery; uploads remain separate |
| Managed database recovery plus file protection | Provider-shaped controls and application integration | Teams accepting those controls |
| Self-hosted secondary storage | Capacity, durability, replication, pager | Explicit residency or control requirements |

If uploads already live under immutable object keys, avoid copying unchanged bytes into every logical snapshot. Preserve the full inventory in the manifest and use versioning or replication controls appropriate to the retention policy. Requirements vary with data shape and regulation, so the measured drill should decide where a nightly design ends.

## References

- https://www.postgresql.org/docs/current/app-pgdump.html
- https://www.postgresql.org/docs/current/continuous-archiving.html
- https://datatracker.ietf.org/doc/html/rfc3339
- https://aws.amazon.com/s3/pricing/
- https://docs.digitalocean.com/products/spaces/
