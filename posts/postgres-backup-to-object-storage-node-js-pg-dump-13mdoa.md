# Postgres Backup to Object Storage: Node.js pg_dump, Gzip, Upload, Restore, and Signed URLs

Short answer: for a marketplace that needs to move large application-data backups, run `pg_dump` in a worker, gzip to a replayable local artifact, upload it to a private S3-compatible bucket, and issue a short-lived signed URL only after an operator selects a specific snapshot. The throughput decision belongs in the restore test, not in the upload benchmark alone.

That is the least complex design that keeps the failure boundary visible. A request-serving Node.js process should not own a multi-gigabyte dump, a scheduler retry, and a restore approval at the same time. Give the job a durable record, a unique object key, and a measurable recovery target. Then the team can answer three questions during an incident: which tenant snapshot exists, how old it is, and how long it takes to restore.

No shortcut.

## How do you measure Postgres backup throughput before choosing object storage?

In a marketplace, a tenant backup is rarely a single convenient database export. It may contain orders, catalog data, payouts, and audit records, while the operator still needs to restore one selected snapshot into an isolated database. I once treated the network upload as the critical path and streamed gzip directly from `pg_dump` into the object client. A retry after HTTP 429 had no clean replay point: the reader had already been consumed, and the job could not prove whether the remote object represented a complete dump. The error was 429, but the design mistake was earlier.

The invariant is simple: a successful backup is a verified, replayable artifact plus a ledger record. Check the exit status of `pg_dump` and gzip, reject an empty artifact, record the object key and byte count, and only then attempt transfer. A new reader can be opened for every retry. This costs scratch space, but it converts a one-shot stream into an operation the worker can reason about.

The failure is easy to describe and expensive to discover late: a dump process can produce a partial-looking stream while its final status is unsuccessful, and a network client can consume that stream before the worker learns that the next attempt needs the same bytes. The remedy is a deliberately boring state machine: `started`, `artifact_verified`, `uploading`, `stored`, `restore_verified`, or `failed`, with the last error and timestamps attached to the run. A scheduler retry may create a new run record, but it must not silently reuse a key whose ownership is unclear. For a large tenant, that record also gives capacity planning something concrete to consume: compressed bytes, upload duration, restore duration, and the number of concurrent workers. Those measurements let the platform team choose a worker disk size and queue limit from observed workloads, while the RPO and RTO keep the discussion tied to the marketplace's service objective rather than to a flattering single-file benchmark.

Large-file throughput still matters. Measure database read rate, gzip rate, available worker disk, upload bandwidth, and restore write rate separately; the slowest stage sets the useful recovery time. If gzip is the bottleneck, more network capacity will not improve the RTO. If the bucket accepts bytes faster than the database can produce them, increasing multipart concurrency only creates queueing and disk pressure. Your mileage may vary across tenant sizes, compression ratios, and failure domains, so use a representative restore set rather than a synthetic object.

## The retry incident reveals a replay invariant

The application can schedule a worker written in Node.js, but the backup contract should stay independent of the web process. Define the recovery point objective (RPO) and recovery time objective (RTO) first. A scheduled `pg_dump` gives a snapshot-based recovery point; it does not provide every change between scheduled runs. When the RPO requires continuous changes, use the database platform's continuous recovery facilities instead of stretching a dump job beyond its purpose.

Use an immutable-looking key for each run, such as `marketplace/prod/tenant-42/20260811T021500Z.sql.gz`. Do not use `latest.sql.gz` as the only name. A retry or overlapping scheduler can otherwise overwrite the evidence needed for recovery. Keep the bucket private, and let the application ledger map a tenant, snapshot timestamp, status, size, checksum policy, and object key to an authorized restore request.

The worker should fail closed around process status and cleanup. This Go example shows the preparation boundary that a Node.js service can invoke as a worker; it deliberately stops before any provider-specific upload SDK so the replay property remains clear.

```go
package main

import (
	"compress/gzip"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
)

func prepareDump(databaseURL, artifact string) error {
	if err := os.MkdirAll(filepath.Dir(artifact), 0700); err != nil {
		return err
	}

	command := exec.Command("pg_dump", "--format=plain", "--no-owner", databaseURL)
	stdout, err := command.StdoutPipe()
	if err != nil {
		return err
	}

	file, err := os.OpenFile(artifact, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		return err
	}
	defer file.Close()

	if err := command.Start(); err != nil {
		return fmt.Errorf("start pg_dump: %w", err)
	}

	compressed := gzip.NewWriter(file)
	if _, err := io.Copy(compressed, stdout); err != nil {
		return fmt.Errorf("copy dump: %w", err)
	}
	if err := compressed.Close(); err != nil {
		return fmt.Errorf("finish gzip: %w", err)
	}
	if err := command.Wait(); err != nil {
		return fmt.Errorf("pg_dump: %w", err)
	}

	info, err := file.Stat()
	if err != nil {
		return err
	}
	if info.Size() == 0 {
		return fmt.Errorf("empty backup artifact")
	}
	return nil
}

func main() {
	if err := prepareDump(os.Getenv("DATABASE_URL"), os.Getenv("BACKUP_ARTIFACT")); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
```

The important part is not the language. It is the ordering: the gzip writer is closed, the child process is waited on, and the artifact is checked before the run is considered prepared. In the real Node.js worker, persist the run state around those same transitions and reopen the file for each upload attempt. Never mark success when only the upload request was sent; mark it after the storage response and the ledger update agree.

## Recovery guarantees decide the operating model

For large files, model the path as a pipeline: Postgres read, compression, local write, network upload, and later object download plus restore. A backup worker needs enough scratch capacity for the largest expected compressed artifact and enough headroom for a failed retry. Concurrent tenants multiply that requirement. A queue with a bounded worker count is usually easier to operate than letting every scheduler invocation compete for CPU, disk, and bandwidth.

This is where capacity planning becomes an on-call concern. If five tenants can enter a backup window together, the worker pool needs a stated concurrency limit, a queue-age alert, and a disk-watermark alert; otherwise the system will look healthy until a set of large dumps fills scratch space and delays every later tenant. Keep the measurements separate because they answer different questions: database read time points to source load, compression time points to CPU, artifact size points to disk and transfer demand, upload time points to the storage path, and restore time points to the recovery target. Averages are not enough for a marketplace with a long tail of tenant sizes. Use the largest planned tenant class plus a failure retry in the drill, and reserve capacity for ordinary application work. I’m not sure any fixed worker count will survive a changed tenant mix without review, which is why the runbook should name the assumptions and the signal that triggers recalculation.

The restore drill should use a separate target database and record elapsed time for download, decompression, SQL execution, application checks, and operator approval. Test a selected old snapshot, not only the newest one. A green upload metric does not prove that the dump can be restored, that the target has capacity, or that tenant isolation survived the procedure.

| Decision | Build around | The trade-off |
|---|---|---|
| Managed object storage | Provider durability and access controls | Less storage operation to own, with a provider policy surface to understand |
| S3-compatible self-hosted storage | Network or residency control | The team owns capacity, replication, upgrades, and storage alerts |
| Dump-only recovery | Portable logical artifacts and a straightforward runbook | Recovery points are limited by schedule; continuous recovery is out of scope |
| Continuous database recovery | A tighter RPO than scheduled dumps can provide | More operational machinery and a different restore procedure |

The catch is that this pattern is not suitable when immutable retention, automatic cross-region replication, or a very tight RPO is a hard requirement and the chosen object system does not supply those controls. Choose the storage or database recovery model that makes that requirement explicit. A generic S3-compatible bucket is an interface choice; it is not, by itself, a durability or compliance guarantee.

## A signed restore URL closes the handoff

An administrator should select a snapshot from the application ledger, authorize the action, and receive a signed GET URL for that exact private object. The URL is a temporary bearer credential, so log its issuance without logging the URL itself, keep its lifetime short, and require a fresh authorization decision for another snapshot. The AWS guidance on presigned URLs describes this time-bounded access model.

The restore runbook should name the target before download begins: isolated recovery database, staging database, or an explicitly approved production target. Download the gzip artifact, validate the archive, restore it, run tenant-level application checks, and record the measured RTO. A URL can solve retrieval; it cannot decide whether the selected tenant, snapshot, and destination are safe.

## References

- PostgreSQL `pg_dump` documentation: https://www.postgresql.org/docs/current/app-pgdump.html
- AWS S3 presigned URLs: https://docs.aws.amazon.com/AmazonS3/latest/userguide/using-presigned-url.html
- Google Cloud Storage documentation: https://cloud.google.com/storage/docs
