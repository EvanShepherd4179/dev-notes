# Failure Budgets for Browser Direct Upload: Node.js, Multipart Signing, and Object Storage

**Short answer:** Treat a large browser direct upload as a small distributed transaction: let the application control plane initiate it, authorize each part, record progress, and make exactly one explicit decision to complete or abort. The browser should move bytes straight to the S3-compatible object store, while a Node.js service owns identity, object naming, policy, and the final state transition. Presigned parts reduce pressure on application servers; they don't remove the need for lifecycle control.

The operational recommendation is blunt: never hand the browser a credential, never infer completion from client-side progress, and never leave cancellation as a tab-close side effect. A progress bar is presentation. The storage service's multipart state is reality.

## Why does a large file browser direct upload need multipart presigned parts?

A single request ties success to one long-lived connection and gives the application little room to retry a bounded unit. Multipart upload changes the failure domain. The control plane starts an upload, the browser sends independently authorized parts directly to storage, and the control plane later submits the ordered part results for completion. If the workflow is abandoned, it aborts the upload. That initiate, upload, complete-or-abort sequence is the durable model described in the Amazon S3 multipart upload overview.

The distinction between data plane and control plane matters more than the framework. The control-plane duties are a narrow application API, user authentication, server-generated object keys, and part authorization. Node.js is merely one runtime for those duties. The browser receives only the URLs and metadata needed for this upload. It doesn't get a reusable storage secret.

Presigned URLs are capabilities, so scope them to one method, one object, and one part. Keep their lifetime long enough for the slow edge of the supported client population, but short enough that an abandoned plan does not remain useful indefinitely. There isn't one defensible duration for every product; resolve it from observed part-upload latency, retry policy, client bandwidth distribution, and the security team's replay assumptions. I'm not sure a default chosen without those measurements means much.

Short parts improve retry granularity but create more requests and more state. Large parts reduce request count but make every retry heavier. Capacity planning therefore starts with concurrent uploaders multiplied by parts in flight, not with average object size alone. Put explicit ceilings on file size, part count, per-user concurrency, and total signing rate, then load-test the control plane at the admission limit. Don't let the browser select those ceilings.

## Make the upload session the unit of correctness

Store an application upload record before returning any part URL. At minimum, it needs the authenticated owner, immutable object key, storage upload identifier, expected content type or class, creation time, and a state such as `open`, `completing`, `complete`, or `aborted`. Part numbers and returned entity tags belong to the same logical session, even if the browser submits them only at finalization.

The key invariant is simple.

**Only the control plane may move a session to a terminal state.**

That invariant closes several awkward gaps. A user cannot complete somebody else's upload merely by learning an identifier. A retry of the finalize request can be reconciled against stored state. A cancellation can be authenticated and audited. Object keys stay server-generated, which prevents one client from choosing a key that overwrites another tenant's data.

The completion request must contain the part numbers and the storage-returned entity tags in ascending part order. Validate that the session is still open, belongs to the caller, and refers to the same immutable object key before asking storage to complete it. Treat malformed manifests, duplicated part numbers, missing authorization, and expired presigned requests as client-visible failures; an HTTP `403` from a part PUT is not evidence that the whole session is complete, and blindly regenerating an entire plan can multiply work. Issue a replacement authorization only after rechecking session state.

Completion is a commit.

It also needs idempotency at the application boundary. Move `open` to `completing` with a conditional write, allow one worker to perform the storage transition, and make concurrent callers observe the stored result. The precise reconciliation call depends on the S3-compatible implementation, so test it rather than assuming every compatibility claim covers identical edge behavior. Compatibility is a protocol surface, not an operational SLO.

## A safe control-plane shape

The following Go sketch shows the boundary that a Node.js implementation should preserve. The interfaces deliberately hide SDK-specific request types; production adapters can use the selected store's supported signing and multipart operations without leaking credentials or vendor objects into route handlers.

```go
package upload

import (
    "context"
    "errors"
    "sort"
    "time"
)

type Part struct {
    Number int
    ETag   string
}

type Session struct {
    ID, Owner, Key, StorageID, State string
    CreatedAt                        time.Time
}

type Store interface {
    CreateMultipart(ctx context.Context, key, contentType string) (string, error)
    PresignPart(ctx context.Context, key, uploadID string, part int, expires time.Duration) (string, error)
    CompleteMultipart(ctx context.Context, key, uploadID string, parts []Part) error
    AbortMultipart(ctx context.Context, key, uploadID string) error
}

type Sessions interface {
    Insert(ctx context.Context, s Session) error
    GetOwned(ctx context.Context, id, owner string) (Session, error)
    CompareAndSwapState(ctx context.Context, id, from, to string) (bool, error)
}

type Service struct {
    blobs Store
    db    Sessions
}

func (s Service) Complete(ctx context.Context, id, owner string, parts []Part) error {
    session, err := s.db.GetOwned(ctx, id, owner)
    if err != nil {
        return err
    }
    if session.State == "complete" {
        return nil
    }
    if session.State != "open" || len(parts) == 0 {
        return errors.New("upload is not completable")
    }

    sort.Slice(parts, func(i, j int) bool { return parts[i].Number < parts[j].Number })
    for i, part := range parts {
        if part.Number < 1 || part.ETag == "" || (i > 0 && parts[i-1].Number == part.Number) {
            return errors.New("invalid part manifest")
        }
    }

    won, err := s.db.CompareAndSwapState(ctx, id, "open", "completing")
    if err != nil {
        return err
    }
    if !won {
        return errors.New("completion already in progress")
    }

    if err := s.blobs.CompleteMultipart(ctx, session.Key, session.StorageID, parts); err != nil {
        return err
    }
    _, err = s.db.CompareAndSwapState(ctx, id, "completing", "complete")
    return err
}

func (s Service) Abort(ctx context.Context, id, owner string) error {
    session, err := s.db.GetOwned(ctx, id, owner)
    if err != nil {
        return err
    }
    won, err := s.db.CompareAndSwapState(ctx, id, "open", "aborted")
    if err != nil || !won {
        return err
    }
    return s.blobs.AbortMultipart(ctx, session.Key, session.StorageID)
}
```

The sketch leaves one important production concern visible: if the storage completion succeeds but the final database write is interrupted, the session remains `completing`. Do not convert that ambiguous state back to `open` automatically. Reconciliation should inspect authoritative storage state through the chosen adapter and then converge the record to `complete` or an explicitly reviewable state. The same principle applies to abort. This is where a clean interface earns its keep — storage-specific reconciliation stays below the application workflow.

A browser may upload several parts concurrently, retain each returned entity tag, retry an individual failed PUT under a bounded backoff policy, and send the ordered manifest to the control plane. It should stop scheduling new work when the user cancels, call the authenticated abort route, and surface failure if abort cannot be confirmed. No drama.

## Verify the SLO before enabling full traffic

Test against the exact S3-compatible service and region used in production. A useful acceptance suite creates a multipart upload, signs a part, uploads known bytes, completes the manifest, and verifies the final object's expected metadata and content through an authorized read path. A second case aborts after at least one uploaded part and confirms that the application session is terminal. Add negative cases for another user's session, duplicate part numbers, an empty manifest, an expired signature, and two simultaneous completion requests.

Observe the workflow as state transitions, not as a pile of route timings. Track admitted sessions, open-session age, part-signing latency, completion latency, abort outcomes, reconciliation backlog, and the ratio of terminal sessions to initiated sessions. Alert on sustained growth in old open sessions or `completing` sessions; those signals point to retained storage work and ambiguous customer outcomes even when ordinary HTTP availability looks healthy.

For rollout, begin with conservative concurrency and object-size limits, then raise them while watching tail latency and signing capacity. Rollback should disable new initiations while leaving completion and abort available for sessions already admitted. Turning off every route at once strands work. Keep cleanup and reconciliation running until the open-session inventory reaches the agreed threshold.

A lifecycle rule that removes stale multipart work is valuable defense in depth, but it isn't the primary cancellation mechanism. The application still needs an abort path because lifecycle timing and application intent are different things.

## Should the team buy the control plane or build it?

The decision is about ownership, on-call load, and portability rather than syntax. A thin in-house control plane is attractive when identity, object naming, retention, and admission policy are already platform concerns. A managed upload layer may reduce implementation work, but its session model, observability, and exit path need the same scrutiny as its happy-path API.

| Decision axis | Build a thin control plane | Adopt a managed upload layer |
|---|---|---|
| Policy fit | Direct control over tenant, key, and quota rules | Faster only when its policy model matches yours |
| Operations | Your team owns signing capacity, reconciliation, and cleanup | Provider owns more machinery; your team still owns integration SLOs |
| Portability | Generic interfaces can isolate store-specific behavior | Session APIs and callbacks may increase switching work |
| Incident scope | More components are directly visible and debuggable | Fewer components to run, with an external dependency in the path |

The catch is that a custom control plane is not suitable when the team cannot staff reconciliation, abuse controls, and cleanup. In that case, use a managed layer whose documented limits and failure semantics satisfy the workload, and retain an exportable application record for every upload. Conversely, stick with direct storage primitives when minimizing external workflow dependencies matters more than saving implementation time. Neither choice removes the need to test complete and abort behavior.

Cost belongs in capacity review, but request pricing alone is a poor selection rule. Model storage retained by unfinished uploads, signing traffic, completion and abort calls, data transfer, support, engineering time, and on-call burden. Pricing changes, and workload shape usually dominates a single headline rate.

## References

- https://docs.aws.amazon.com/AmazonS3/latest/userguide/mpuoverview.html
- https://www.backblaze.com/cloud-storage/pricing

## Further reading

The multipart overview above is the primary protocol reference; the pricing page is useful when building a current cost model.
