# Node and Express: retaining user-generated AI image objects with lifecycle expiry

Use object storage with a stable, per-user key scheme; let a bucket lifecycle policy age routine generated images, and reserve an application delete path for immediate revocation. That is the least complex design that still gives an AI image service a defensible retention story. A “folder” is only a key prefix, so the prefix must carry the tenant boundary your access policy, database, and cleanup work can all verify.

The hard part is not putting bytes somewhere. It is making deletion, authorization, and capacity behavior agree after the service has accumulated enough generated images that a one-off script is no longer a credible control.

## The incident lesson: an apparently healthy cleanup job is not evidence

Here is the bounded scenario I use in design reviews. A Node and Express endpoint accepts an AI-generated image, writes an object, and a nightly worker lists old objects before deleting them. The job emits `scanned=250000 deleted=0`, exits successfully, and the dashboard stays green. A month later, storage has no retention plateau.

Nothing crashed.

The failure is usually architectural: the worker is trying to infer tenant ownership from an object listing, an optional metadata field, or a name reconstructed by a newer version of the application. Those are weak contracts for a destructive path. An object store can list keys under a prefix; it does not turn an application’s forgotten ownership model into a reliable schema.

The invariant is plain: place an immutable internal user ID in the object key, store that exact key in the image row, and have every control use the same representation. A key such as `users/42/generated/2026/08/asset-id.webp` permits prefix-scoped authorization and inspection without putting an email address in URLs, logs, or support exports. The date is useful for human investigations and partitioned scans; the opaque asset ID prevents collisions and guessing. Do not rebuild historic keys from a formatter during deletion. Read the persisted key instead.

I would alert on a retention outcome, not merely job success: expected eligible objects versus observed eligible objects, current bytes versus the planned steady-state range, and the age of the oldest object outside an allowed exception set. The exact thresholds depend on image size, active users, and the retention contract; I'm not sure a generic number would survive contact with a real workload. The missing input is a measured ingest distribution, including retries and images deliberately pinned by users.

Make the verification exercise unpleasantly concrete before connecting it to production. Create a test user with objects older and newer than the stated retention boundary, an image whose database row is pending, an image marked unavailable, and a deliberately duplicated delete message. List only that user's prefix, compare every returned key to the repository, then run the lifecycle policy in its documented test or staging mode where available. Check that a read authorization decision denies the unavailable image before physical deletion completes, because storage removal and access revocation have different propagation paths and different evidence. Then replay the delete message, inspect the completed state, and verify that the monitoring view distinguishes a lifecycle-aged object from a user-requested deletion. This is less glamorous than a load test, but it tests the contract an operator will need under pressure: which keys exist, which ones may be served, which deletion mechanism owns them, and what evidence remains after a retry. A green HTTP response proves almost none of that.

Measure it twice.

## How should Node and Express store AI-generated images before lifecycle deletion?

Keep the request handler responsible for identity, validation, quota admission, and recording the immutable object key. Keep object transport out of the handler when the client can upload directly with a narrowly scoped, short-lived authorization. That reduces Node memory pressure and makes a retry of the byte transfer independent from a retry of the database transaction.

There are two state transitions to design deliberately. First, create an image record in a pending state and allocate its key. Next, write the object and mark the record available only after the write succeeds. A read path must require the available state. This leaves recoverable residue in either direction: an object without an available row can be reconciled; a pending row can expire or be retried. The alternative, treating a successful upload as proof that the application has recorded ownership, creates an authorization and support problem precisely when requests are interrupted.

The following Go-shaped interface keeps the storage implementation generic while making the ordering and the key contract visible. It is intentionally not an Express tutorial; the HTTP framework should not decide the retention model.

```go
package images

import (
	"context"
	"fmt"
	"time"
)

type ObjectStore interface {
	Put(context.Context, string, []byte, string) error
	Delete(context.Context, string) error
}

type ImageRepository interface {
	CreatePending(context.Context, string, string, time.Time) error
	MarkAvailable(context.Context, string) error
	MarkDeleteRequested(context.Context, string) error
}

func objectKey(userID, imageID string, createdAt time.Time) string {
	d := createdAt.UTC()
	return fmt.Sprintf("users/%s/generated/%04d/%02d/%s.webp",
		userID, d.Year(), d.Month(), imageID)
}

func recordAndStore(ctx context.Context, repo ImageRepository, store ObjectStore, userID, imageID string, body []byte, now time.Time) (string, error) {
	key := objectKey(userID, imageID, now)
	if err := repo.CreatePending(ctx, imageID, key, now); err != nil {
		return "", err
	}
	if err := store.Put(ctx, key, body, "image/webp"); err != nil {
		return "", err
	}
	if err := repo.MarkAvailable(ctx, imageID); err != nil {
		return "", err
	}
	return key, nil
}
```

The user ID above is an internal immutable identifier, not a display name. The service should also validate the media type from the bytes it receives, limit image dimensions and size before admission, and log a request identifier with the key rather than logging signed URLs. Secrets deserve the same restraint: use short-lived workload credentials where the platform supports them, scope permissions to the required prefix and operations, rotate credentials, and keep secret material out of source code and ordinary logs. OWASP’s guidance is a useful baseline for the operational side of that work.

## Lifecycle handles aging; the application handles intent

Lifecycle expiration is the right primitive for a broad policy such as “generated images without an explicit retention exception become eligible after N days.” It applies without a scheduler fleet, pagination loop, or a broad delete credential in a request-serving process. It should be configured against a purpose-specific prefix or classification so that a future object type does not inherit a destructive default by accident.

Immediate user deletion is a different SLO. Mark the image unavailable first, so authorization stops serving it, then enqueue an idempotent worker that deletes the persisted key. A retry must be safe when the first delete reached the object store but the worker lost its acknowledgement. Record the delete request and completion timestamps so support and compliance work has an auditable state transition rather than a best-effort promise.

The catch is that lifecycle is not suitable when the policy is “keep the newest 20 images per user,” “retain each image until a case closes,” or “delete exactly when a user revokes access.” Those rules require application state and often a queue. Conversely, stick with a lifecycle policy for simple age-based bulk retention; replacing it with a nightly full-bucket scan adds an on-call dependency without adding meaning.

| Decision | Prefer | Operational proof | Boundary |
| --- | --- | --- | --- |
| Age-based expiry for generated output | Lifecycle policy on a dedicated prefix or classification | Eligible-object age and storage trend | It cannot express per-user ranking or business state |
| Immediate removal request | Authorization revocation plus idempotent delete worker | Queue age, retry count, and completed delete records | Requires a persisted key and replay-safe worker |
| Legal or user-selected hold | Database retention state plus a separate protected classification | Hold inventory and periodic access review | Needs explicit governance, not a date-only rule |

If uploads can become large, account for incomplete multipart uploads too. Amazon S3 documents multipart upload as a three-step process and recommends considering it for objects around 100 MB or larger; uploaded parts remain associated with the upload until it completes or is aborted. A lifecycle rule that aborts incomplete multipart uploads is therefore a capacity control, not cosmetic housekeeping. Your storage system may implement similar semantics differently, so verify its lifecycle and multipart documentation before using a rule as a compliance guarantee.

## Capacity and isolation are separate decisions

Start capacity planning with ingestion, retention, and overhead: average encoded image bytes multiplied by successful images per day, then by retained days, plus a margin for retries, previews, and images held outside the normal policy. The resulting steady-state range tells you whether the system needs ordinary monitoring or a more serious archival and budget discussion. It also turns an opaque storage bill into a forecast that can be compared with observed bytes.

A per-user prefix supports organized addressing and prefix-scoped permissions. It is not, by itself, a hard isolation boundary. Choose stronger account, bucket, encryption-key, or deployment separation when the required blast radius or regulatory boundary demands it; those designs add provisioning, policy review, and quota work, which may be justified. Shared galleries also complicate the prefix model because an object can have several viewers while retaining one owner. Keep the ownership record separate from the delivery path rather than copying images casually and hoping every copy follows the same lifecycle.

Do a recovery exercise before calling this done. Restore an accidentally removed record from your backup plan, replay a duplicate delete request, verify an expired object is no longer authorized for delivery, and confirm the metrics would detect an ever-growing old-object population. Small drills expose mismatched assumptions much earlier than a capacity review.

## References

- https://cheatsheetseries.owasp.org/cheatsheets/Secrets_Management_Cheat_Sheet.html
- https://docs.aws.amazon.com/AmazonS3/latest/userguide/mpuoverview.html

## Further reading

- https://cheatsheetseries.owasp.org/cheatsheets/Secrets_Management_Cheat_Sheet.html
- https://docs.aws.amazon.com/AmazonS3/latest/userguide/mpuoverview.html
