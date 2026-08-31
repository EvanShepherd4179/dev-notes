# Startup avatar storage: compatible app cleanup and thumbnails

**Short answer:** An avatar upload system remains operable when durable originals, abandoned uploads, and regenerable thumbnails have different retention rules, and lifecycle cleanup touches only data whose disposal can be proved.

The cheapest-looking byte price is secondary because a storage system that cannot explain its deletes will consume platform time during the first serious incident. For user avatar and file upload paths, start capacity planning with peak concurrent uploads, the maximum accepted source size, and the count of derived variants; those numbers set a queue budget and an object-count ceiling long before they become a bill. An application using an S3-compatible API can preserve a narrow object-store boundary, but API compatibility does not make lifecycle timing, versioning behavior, or multipart-upload cleanup identical. Verify those semantics in the storage documentation before treating a lifecycle policy as a control. The design question is more basic: can an operator trace every object back to a database row or to a defined temporary class? If that answer depends on a log search, a vague timestamp, or an upload handler that assigns a new random key on every retry, cleanup already has an unbounded failure mode. The names, commit order, worker retry policy, and deletion evidence are one operational contract, even if different teams own them.

Keep it boring.

## What should lifecycle cleanup prove about avatar image storage and app thumbnails?

Give each accepted original a stable object key, keep that key in the application database, and separate it from temporary and derived objects by prefix.

The invariant is plain: originals are durable state; thumbnails are reproducible output; uploads that never gained a database reference are temporary. Mixing those classes under one broad expiration rule is how a cleanup job turns into a user-visible data-loss event.

Use names that reveal ownership and purpose. A layout such as `originals/{account}/{digest}`, `derived/{account}/{digest}/{size}`, and `staging/{upload-id}` gives a reconciler an understandable surface. A digest makes a retry idempotent when the same bytes arrive again, while the account namespace avoids making cross-user deduplication an accidental privacy decision.

The important part is the order of work. Store the bounded original, commit its key as the current avatar in a transaction, and generate thumbnail variants asynchronously. The request path stays small enough to protect its latency SLO; the worker can retry without changing the pointer to the original. A failed derived image is cache debt, not lost profile state.

```go
package avatar

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
)

type ObjectStore interface {
	Put(context.Context, string, []byte, string) error
}

func originalKey(accountID string, body []byte) string {
	sum := sha256.Sum256(body)
	return fmt.Sprintf("originals/%s/%s", accountID, hex.EncodeToString(sum[:]))
}

func acceptOriginal(ctx context.Context, store ObjectStore, accountID string, r io.Reader, limit int64) (string, error) {
	body, err := io.ReadAll(io.LimitReader(r, limit+1))
	if err != nil {
		return "", err
	}
	if int64(len(body)) > limit {
		return "", fmt.Errorf("source image exceeds configured limit")
	}
	key := originalKey(accountID, body)
	if err := store.Put(ctx, key, body, "image/webp"); err != nil {
		return "", err
	}
	return key, nil
}
```

This example intentionally stops at the object write. The caller still needs image decoding limits, content validation, authorization, a database pointer update, and an outbox or queue record for thumbnail work. Don't let a tidy helper hide those boundaries.

## The incident lesson is an ownership problem, not a bucket problem

The recurring production failure is easy to model. A mobile client times out after the server has written an object but before it receives the response, retries, and the second request creates a different key. The profile row eventually points at one object, while the first remains invisible to the product and expensive to reason about. No storage vendor caused that outcome; the key-generation and commit protocol did.

Deterministic keys reduce duplicate writes for identical bytes, but they do not prove that every stored original is referenced. A periodic reconciler should compare the application index with a prefix listing, report candidate orphans, and delay destructive action long enough to cover replication lag, transaction retries, and restore procedures. Measure the age of the oldest unreferenced object and the gap between referenced-row count and object count. Those are better operational signals than total gigabytes.

Keep deletion off the avatar replacement request. The old pointer may be read by a concurrent request, and inline deletion makes a database race visible as a broken image. A sweep with an explicit grace period is slower, but its blast radius is bounded and its decisions are auditable.

Short version: delete only objects whose class and ownership you can prove.

## Lifecycle rules should erase cache and abandoned work

Lifecycle cleanup is a useful garbage collector for `staging/` and `derived/`; it is a poor substitute for reference-aware deletion of originals. Scope every rule to a prefix, give it an owner, and test it against synthetic keys before enabling it. The cleanup policy should also address incomplete multipart uploads, since a normal object listing is not the full accounting surface for an upload system.

One operational wrinkle matters during capacity planning. Lifecycle expiration is generally asynchronous, so a configured age is not a precise wall-clock deletion promise. A compliance requirement with a hard deadline needs a verified delete workflow and evidence trail, not an assumption that a scheduled sweep will run at a particular minute. Your mileage may vary with versioning, legal holds, replication, and the storage service's published semantics.

Thumbnails need their own readiness SLO. If the first request after expiry performs resizing, protect the transform worker with a bounded queue, request coalescing for the same key and size, and a concurrency limit derived from memory rather than CPU alone. Image decoders can expand a small upload into a large in-memory bitmap. That is where a seemingly tiny avatar feature runs out of headroom.

## Buy, build, and the cases where this plan does not fit

| Approach | Team owns | Operational trade-off | Fit boundary |
|---|---|---|---|
| Managed object storage | Object naming, access policy, retention verification | Low storage on-call load; provider semantics still require review | A reasonable default when the team does not operate disk replication |
| Self-hosted object storage | Disks, replication, upgrades, restores, and the API | More control, plus a real on-call commitment | Use it when the team has tested recovery and has a durable reason to own the storage layer |
| Database-held image bytes | Schema, backups, and read path | Fewer components initially; larger backups and write amplification | Useful for a very small bounded corpus with restore-time limits understood |
| Dedicated image processing service | Source-of-truth contract and transform authorization | Less transform plumbing; another external contract | Use it when variant policy is complex enough to justify the dependency |

The catch is that private-object storage plus delayed cleanup is not suitable when the product must prove deletion within a fixed short window. In that case, stick with a design whose deletion audit and retention guarantees have been reviewed by the people responsible for the requirement. Likewise, self-hosting is not a cost optimization for a small platform team unless disk failure, restore drills, and pager coverage already have owners.

Avoid choosing from a price table alone. Price pages change, while a capacity model lasts: estimate retained originals, temporary-upload churn, variant count, expected reads, and the egress path. Then run a failure exercise: client retry, worker duplicate delivery, aborted multipart upload, a deleted user, and a cache purge. If the answers name a key, an index, and an accountable job for each case, the design is ready for a small app to grow.

## References

- https://developers.cloudflare.com/r2/
- https://www.backblaze.com/cloud-storage/pricing
