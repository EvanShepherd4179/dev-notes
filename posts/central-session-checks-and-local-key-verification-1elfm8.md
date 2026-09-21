# Central Session Checks and Local Key Verification for Marketplace Account Erasure

**Decision rule:** use local key verification for routine service-to-service authorization only when the maximum accepted token lifetime fits the revocation objective; route deletion, recovery, and other account-state transitions through a central session check. For a marketplace deleting an account under GDPR, do not pretend that a cached public key can report a session revocation. It proves a signature. It does not prove that the account still exists.

**TL;DR:** split the path by consequence. Let high-volume internal reads verify short-lived JWTs locally, but make account deletion a deny-first state transition: mark the account unavailable, revoke its sessions centrally, stop new token issuance, propagate deletion work, and require central state for every recovery attempt. Track the interval from accepted deletion to the last successful authenticated request as an SLO. That interval, rather than average JWT verification latency, is the capacity-planning number that decides whether the design is acceptable.

## Why can't local verification see a revoked session?

A service performing local JWT verification checks the token's signature and claims against locally available keys and policy. RFC 7519 defines the JWT claims format, including expiration, while RFC 8725 describes validation requirements and common implementation failures. Neither turns a signed token into a live query against account state. If a token was validly issued for a seller and the seller is deleted one second later, the token remains cryptographically valid until an enforced time boundary or an additional state check rejects it.

This distinction matters in a marketplace because deletion has several moving parts: buyer and seller sessions, background jobs, pending orders, audit records, and a recovery path that must not quietly recreate access. The erasure workflow may retain records where another legal obligation applies, but authentication must still move to deny-by-default immediately. GDPR Article 17 describes the right to erasure and its exceptions; it does not make JWT expiry a revocation mechanism.

A central check changes the failure model. Every protected call, or every call selected by policy, asks authoritative session state whether the token identifier, subject, or session family remains active. Revocation can take effect on the next successful check, but availability and tail latency now include that dependency. Local verification removes that request from the hot path, yet its revocation lag is bounded by token lifetime plus clock and propagation allowances. One buys fresher state with a network dependency; the other buys isolation with a stale-authorization window.

No magic here.

The limitation is blunt.

Suppose a locally verified service token has a five-minute lifetime. It is issued at 12:00:00, the marketplace commits account deletion at 12:00:01, and a catalog service receives the token at 12:04:59. Signature, issuer, audience, and expiration checks can all succeed; none includes the deletion committed almost five minutes earlier. Shortening the lifetime to one minute reduces that particular window, but multiplies issuance and refresh traffic and still leaves a nonzero interval. Adding a deny-list can close the interval only when every relevant service receives or queries it within the required time, at which point its propagation delay, cache policy, and failure behavior are part of the authorization system. This is why the decision must be stated as a time bound. Don't label local verification "revocation aware" merely because the token expires soon.

## Choose the boundary from the revocation SLO

Start with a concrete objective: after the deletion endpoint accepts a request and commits the account's disabled state, no authenticated marketplace operation may succeed beyond the declared revocation window. Measure p99 and worst observed propagation separately. Averages hide the one request that places an order after deletion.

The choice is easier to defend as a capacity and failure-budget table than as a blanket rule.

There is no universally faster architecture once the whole request is counted. Central checks are a poor fit for disconnected workers, very high-volume low-risk reads, or any path whose availability objective cannot absorb an authorization-state dependency. Local checks are a poor fit when the permitted stale-access window is zero or shorter than the operationally sustainable token lifetime. **The trade-off is fresh denial versus request-path independence**, and a hybrid policy accepts the complexity of operating both.

| Decision factor | Central session check | Local key verification |
|---|---|---|
| Revocation freshness | Can observe authoritative denial on the next successful check | Bounded by token expiry or a separate deny signal |
| Request-path latency | Adds a network hop or cache lookup | Avoids a per-request authorization network hop |
| Failure behavior | Must define fail-closed behavior and dependency budgets | Can continue during control-plane isolation while keys and policy remain usable |
| Capacity driver | Protected request rate, cache hit rate, and tail latency | Token verification CPU, key refresh load, and permitted token lifetime |
| Recovery safety | Naturally consults current account state | Requires recovery to bypass token validity and consult authoritative state |
| Operational burden | Highly available state, overload control, and observability | Key rotation, strict claim validation, short expiry, and bounded cache age |

For service-to-service traffic, classify operations rather than services. Catalog reads may tolerate a short stale window; deleting an account, changing payout details, issuing a recovery credential, or minting another token should consult current state. This hybrid boundary limits central capacity to calls whose correctness depends on fresh revocation, while short-lived access tokens contain exposure elsewhere.

Set the token lifetime from the revocation objective, not developer convenience. If the business cannot tolerate a locally accepted token after deletion, no finite lifetime repairs the mismatch; that route needs a stateful check. Also reject tokens whose issuer, audience, algorithm, time claims, or key are outside explicit policy. OWASP's authentication guidance recommends invalidating sessions after reauthentication and rotating tokens after risk events, which is consistent with treating recovery and deletion as changes in authorization state rather than ordinary API calls.

## Implement deletion as a deny-first transition

The deletion handler should make access impossible before slow erasure work begins. In one transaction, move the account to a non-active state and advance a session generation or equivalent revocation marker. Token issuance and recovery read the same authoritative state. After commit, publish idempotent work for downstream deletion; retries must not reactivate the principal.

The focused Go example below shows the authorization boundary, not a complete JWT library. Signature and claim parsing belong in a maintained implementation that enforces an algorithm allowlist. `VerifyLocal` returns already validated claims; `CheckCentral` consults authoritative session state for sensitive operations.

```go
package authz

import (
	"context"
	"errors"
	"time"
)

var ErrUnauthorized = errors.New("unauthorized")

type Claims struct {
	Subject           string
	SessionID         string
	SessionGeneration uint64
	ExpiresAt         time.Time
}

type TokenVerifier interface {
	VerifyLocal(ctx context.Context, raw string, now time.Time) (Claims, error)
}

type SessionStore interface {
	CheckCentral(ctx context.Context, subject, sessionID string, generation uint64) error
}

type Authorizer struct {
	Tokens   TokenVerifier
	Sessions SessionStore
	Now      func() time.Time
}

type Operation int

const (
	CatalogRead Operation = iota
	PlaceOrder
	DeleteAccount
	RecoverAccount
)

func (a Authorizer) Authorize(ctx context.Context, raw string, op Operation) (Claims, error) {
	claims, err := a.Tokens.VerifyLocal(ctx, raw, a.Now())
	if err != nil {
		return Claims{}, ErrUnauthorized
	}

	if op == CatalogRead {
		return claims, nil
	}

	if err := a.Sessions.CheckCentral(ctx, claims.Subject, claims.SessionID, claims.SessionGeneration); err != nil {
		// Sensitive operations fail closed when fresh session state is unavailable.
		return Claims{}, ErrUnauthorized
	}
	return claims, nil
}
```

Deletion should use the same state transition even when the caller presents a currently valid token. First require recent authentication appropriate to the risk, then atomically disable the account and revoke the session family. A queue consumer can perform slower personal-data deletion, but its backlog must never govern whether authentication remains possible. Keep retained order or tax data behind a purpose-limited authorization boundary rather than leaving the user principal active.

The recovery path is the trap. Email possession, a support decision, or a previously issued recovery code must not clear the deletion marker as a side effect. Model recovery as a separate state machine with explicit outcomes: restore access only if policy and the deletion stage permit it; otherwise create a new principal after the required checks. Any recovery success must rotate sessions, and any failure response should avoid revealing whether the deleted account remains on file.

## Verify the system under failure, not just the happy path

Test with two clocks: token time and control-plane time. Issue a token, begin a request just before deletion commits, then hold it across the commit and release it against every sensitive operation. Repeat during session-store timeout, key rotation, delayed deletion events, duplicate events, and recovery attempts. The acceptance criterion is observable: after the revocation boundary, protected mutations fail; queued cleanup may lag without reopening access.

Monitor at least central-check p50, p95, and p99 latency; timeout and fail-closed counts; locally accepted token age; key-set age; deletion-to-last-authorized-request duration; active sessions remaining after deletion; and recovery attempts against deleted subjects. Avoid logging raw tokens or recovery secrets. Correlate with opaque request and session identifiers whose retention has been reviewed.

Capacity planning needs burst assumptions. Account deletion traffic may be small, but central checks also cover ordinary sensitive requests, and an outage can synchronize retries. Load-test the authorization store at the expected peak plus retry amplification, then cap concurrency and apply jittered backoff outside the user request. A cache is useful only if its maximum staleness still satisfies the revocation SLO; otherwise it quietly converts the central design back into local acceptance.

That cache has a cost.

Roll out by operation class. Shadow the central decision first and compare it with local acceptance without granting access from the shadow result. Investigate every disagreement, then enforce on the highest-consequence operations while watching latency and denial error budgets. Expanding enforcement should be a policy change, not a redeploy across every service.

## Roll back without restoring deleted access

A rollback may disable a new code path, but it must not roll account state backward. Preserve the deletion marker and session generation. If the central checker is unhealthy, sensitive operations fail closed; operators may shed load from lower-priority operations, increase healthy capacity, or temporarily require central checks for fewer low-risk calls, but deletion, recovery, token minting, and monetary mutations keep the authoritative check.

The final decision is deliberately unglamorous: local verification is the fast data-plane primitive, central verification is the fresh-state primitive, and a production design needs both wherever the revocation SLO and recovery policy differ by operation. Treat deletion as an irreversible authorization boundary until a separately audited recovery transition says otherwise.

## References

- https://datatracker.ietf.org/doc/html/rfc7519
- https://datatracker.ietf.org/doc/html/rfc8725
- https://cheatsheetseries.owasp.org/cheatsheets/Authentication_Cheat_Sheet.html
- https://cheatsheetseries.owasp.org/cheatsheets/Session_Management_Cheat_Sheet.html
- https://eur-lex.europa.eu/eli/reg/2016/679/oj
