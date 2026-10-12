# Growth Launch API Spend Cap — Lease-Based Reversion Under Metering Lag

A page fires during a developer-tool launch: customer usage is still climbing, but the shared API budget has crossed its warning threshold and the next control action will refuse traffic. The on-call sees the current spend estimate, the active cap, the override expiry, and the last successful meter checkpoint. If the expiry is missing, the page is already telling us that the change was unsafe.

**TL;DR:** represent the temporary cap increase as a lease with an absolute expiry, record the old value before raising it, and schedule the restore before launch traffic starts. Let a durable worker perform an idempotent compare-and-restore operation, then verify the effective cap independently. The decision is explicit: a higher ceiling limits refused traffic during the launch, while a shorter lease limits invoice exposure if demand or metering behaves differently from the forecast.

Do not make a process timer inside the launch script the only restore mechanism.

Processes restart.

Deployments happen, and the operator who raised the limit may be asleep when the window closes.

## How should a launch temporarily raise and restore an API spend cap?

The spend-cap page is late because it describes a control boundary, not the rate at which the system is approaching it. Work backward from that page. A useful earlier signal combines the metered usage rate, the remaining budget, and the time left in the approved launch window. It should also expose meter freshness; an apparently comfortable runway calculated from stale usage is false comfort.

For a concrete, illustrative launch window, suppose the normal cap is 100,000 billing units, the approved temporary cap is 160,000 units, and the lease lasts 90 minutes. Those numbers are example policy inputs, not a claim about any provider or a recommendation for another workload. Capacity planning starts with the expected customer request rate and units per request, then adds a declared uncertainty margin. The platform team should be able to explain why 160,000 is sufficient and why it is not 300,000.

The alert should distinguish three states. First, projected consumption reaches the temporary ceiling before expiry: traffic may soon be refused, so the on-call must decide whether to protect the spend ceiling or authorize another bounded lease. Second, metering is stale: stop trusting the projection and apply the conservative policy chosen before launch. Third, the lease has expired but the effective cap remains elevated: restoration has breached its SLO and deserves an immediate page. That last state matters most because a launch ending quietly does not prove the guardrail returned, and a green worker log proves only that one process believed its write succeeded; the independent read of the effective cap is the evidence the operator needs.

## Instrument the override as a lease, not a mutable number

A bare update such as `cap = 160000` loses the reason, owner, previous value, and restoration deadline. Store an immutable lease record first, with a unique change ID, account scope, old cap, temporary cap, creation time, expiry, and approval reference. The control-plane update and the lease record may not share a transaction, so the workflow must tolerate a crash between every pair of steps.

The secret used to change a cap belongs in a secrets-management system rather than source code or a job payload. OWASP's Secrets Management Cheat Sheet recommends centralized management, least privilege, rotation, auditing, and controls around secret access. For this job, that means the worker receives only the authority required to read and change the scoped cap; logs retain the change ID and result, never the credential.

The following Go sketch keeps the vendor boundary deliberately small. The durable scheduler is represented by an interface because its delivery semantics vary; correctness stays in the restore handler.

```go
package budgetlease

import (
    "context"
    "errors"
    "fmt"
    "time"
)

type Lease struct {
    ID           string
    AccountID    string
    PreviousCap  int64
    TemporaryCap int64
    ExpiresAt    time.Time
}

type CapStore interface {
    Current(ctx context.Context, accountID string) (int64, error)
    CompareAndSet(ctx context.Context, accountID string, from, to int64) (bool, error)
}

type LeaseStore interface {
    Create(ctx context.Context, lease Lease) error
    MarkRestored(ctx context.Context, leaseID string, restoredAt time.Time) error
}

type Scheduler interface {
    EnqueueRestore(ctx context.Context, leaseID string, runAt time.Time) error
}

func Raise(ctx context.Context, caps CapStore, leases LeaseStore, jobs Scheduler, lease Lease) error {
    if lease.ID == "" || lease.AccountID == "" {
        return errors.New("lease and account IDs are required")
    }
    if !lease.ExpiresAt.After(time.Now()) || lease.TemporaryCap <= lease.PreviousCap {
        return errors.New("invalid temporary cap lease")
    }

    current, err := caps.Current(ctx, lease.AccountID)
    if err != nil {
        return fmt.Errorf("read current cap: %w", err)
    }
    if current != lease.PreviousCap {
        return fmt.Errorf("cap changed before lease creation")
    }
    if err := leases.Create(ctx, lease); err != nil {
        return fmt.Errorf("persist lease: %w", err)
    }
    if err := jobs.EnqueueRestore(ctx, lease.ID, lease.ExpiresAt); err != nil {
        return fmt.Errorf("schedule restore: %w", err)
    }
    changed, err := caps.CompareAndSet(ctx, lease.AccountID, lease.PreviousCap, lease.TemporaryCap)
    if err != nil {
        return fmt.Errorf("raise cap: %w", err)
    }
    if !changed {
        return errors.New("cap changed while applying lease")
    }
    return nil
}

func Restore(ctx context.Context, caps CapStore, leases LeaseStore, lease Lease, now time.Time) error {
    if now.Before(lease.ExpiresAt) {
        return errors.New("lease has not expired")
    }
    changed, err := caps.CompareAndSet(ctx, lease.AccountID, lease.TemporaryCap, lease.PreviousCap)
    if err != nil {
        return fmt.Errorf("restore cap: %w", err)
    }
    if !changed {
        current, readErr := caps.Current(ctx, lease.AccountID)
        if readErr != nil {
            return fmt.Errorf("verify cap after conflict: %w", readErr)
        }
        if current != lease.PreviousCap {
            return fmt.Errorf("restore conflict: effective cap is %d", current)
        }
    }
    return leases.MarkRestored(ctx, lease.ID, now)
}
```

Schedule first, raise last. That ordering leaves a recoverable record and a pending restore if the final update fails. The worker may receive the restore job more than once, so a second run accepts the already-restored value as success. It must not overwrite a newer intentional cap change: compare-and-set turns that case into a conflict for an operator or reconciliation process instead of silently erasing it.

One complication remains. If scheduling succeeds and raising fails, the restore job still exists. That is acceptable because it will see that the temporary value is absent and refuse to overwrite an unrelated value; a reconciliation pass can then close the unused lease. This is less tidy than pretending the calls are atomic, but it is honest about distributed control planes.

No timer can guarantee that.

## Test the recovery paths before the growth spike

The happy path proves little. Inject a worker restart after the lease record is created, after the restore is scheduled, and after the cap is raised. Deliver the restore twice. Delay it. Change the cap through a second approved lease before the first lease expires. Make the meter checkpoint stale while request volume climbs. Each test should end with an unambiguous effective cap, a durable audit record, and a reason when automation declines to act.

A small buy-versus-build review keeps the team from confusing a scheduler with the whole safety mechanism. Product names are irrelevant here because these properties can be evaluated directly.

| Capability | Managed job service | Self-hosted worker |
|---|---|---|
| Durable delivery | Contracted service behavior; verify retry and retention limits | Team owns persistence, retry, and recovery |
| On-call load | Lower infrastructure burden, but control-plane dependency remains | Higher operational burden and more failure modes to rehearse |
| Lock-in | Job schema and identity model may constrain migration | Storage and protocol choices remain under team control |
| Correct restore | Still requires idempotency, compare-and-set, and verification | Still requires idempotency, compare-and-set, and verification |

The choice is not settled by feature count. If the team cannot state a restore SLO and test delivery after a regional or deployment failure, owning the scheduler adds risk. If spend controls must remain operable during an external control-plane outage, accepting more self-hosted complexity may be justified. Put both costs into the roadmap: service fees are visible, while rehearsal time and on-call fatigue are easy to omit.

Limitations are concrete. This lease approach is not appropriate when the upstream API cannot read back the effective cap or conditionally reject a stale update; without either capability, automation can overwrite a newer operator decision. In that case, choose a manual two-person change with a pre-booked restore window, or place the enforceable budget boundary in a gateway the team controls. It is also a poor fit for per-request authorization: use admission control for that job, because a delayed spending meter cannot make a synchronous safety decision. The trade-off is more operator effort or infrastructure ownership, but the missing consistency boundary becomes explicit.

Deployment needs a canary too. Apply a short lease to a non-production account, confirm the raise, wait for expiry, and verify restoration through a read path that does not merely echo the worker's write response. In production, dashboard the count of active leases, leases expiring soon, overdue restores, compare-and-set conflicts, meter age, projected units at expiry, and refused requests. The launch review should assign an owner to every active lease rather than treating the scheduler as ownership.

## Choose the alert threshold by pricing both failure modes

An early threshold buys investigation time, but it also pages on harmless bursts and teaches responders to distrust the signal. A late threshold preserves attention while increasing the chance that the hard ceiling refuses customer traffic before anyone can make a deliberate decision. Neither side is free.

Set the warning from response time, not from an arbitrary percentage. Estimate the worst credible delay for meter ingestion, alert evaluation, paging, human acknowledgement, and an approved control action. Convert that duration into budget units using the launch's high consumption rate, then add the uncertainty allowance chosen during capacity planning. Page when the remaining units fall below that amount. This links the threshold to an operational promise that can be reviewed after the event.

The false-positive budget belongs beside the restore SLO. Track how many pages required action, how many were caused by stale telemetry, and how many arrived while enough runway remained for the normal process. Tightening a threshold without measuring that cost can reduce nominal spend risk while degrading the human system expected to protect it.

The durable rule is straightforward: every elevated ceiling has an owner, an expiry, a pre-created restore job, and an independently observed return to the prior value. The lease limits invoice exposure; the forecast and earlier alert limit refused traffic. Treating those as separate controls makes the launch decision reversible without pretending that metering, scheduling, and control-plane writes fail together.

## Further reading

- [OWASP Secrets Management Cheat Sheet](https://cheatsheetseries.owasp.org/cheatsheets/Secrets_Management_Cheat_Sheet.html)
