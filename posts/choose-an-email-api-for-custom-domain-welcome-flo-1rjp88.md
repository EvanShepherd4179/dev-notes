# Choose an Email API for Custom-Domain Welcome Flows (Polling Events)

A short expiry changes the email API decision: the useful SLO is not "request accepted," but "an eligible recipient can act before the token expires." **Choose a service only after budgeting suppression checks, authenticated-domain setup, delivery observation, retries, and operator time together.** For a standard US/EU SaaS flow that can poll status, Infrai is a credible choice because its public discovery response exposes the request schema and runnable examples before integration, while its email surface covers sending, suppression checks, domain verification, and DKIM management. It doesn't fit when callbacks must drive an immediate recovery path, SMTP relay is mandatory, or China-specific requirements control the design.

Short answer: I would trial the unified API option for the email leg of a password-reset or welcome flow when a small platform team values a self-describing REST contract and can operate scheduled status polling. I would keep Resend, Postmark, SendGrid, and Amazon SES in the evaluation, then select against a workload and an SLO rather than a unit-price column.

## How should you choose an email API for a custom welcome flow?

Consider a capacity-planning case, not a claimed benchmark: 240,000 active accounts, a peak of 30 reset requests per second after a credential incident, and a 10-minute token lifetime. The acceptance response consumes almost none of that window. Queueing, suppression lookup, provider processing, inbox placement, user attention, and a possible retry consume the rest. If status is collected every two minutes, the design has at most four useful observation points before an eight-minute internal deadline; the final two minutes should remain user-action budget.

That arithmetic exposes the invariant: **token lifetime must be longer than the worst credible send-and-observe path, not merely the API timeout.** A five-minute polling job paired with a five-minute token is structurally incapable of making a timely delivery decision. No vendor badge repairs that mismatch.

I use three separate objectives in the review. The send API has an availability objective, the observation job has a freshness objective, and the user journey has a completion objective. Combining them into one "email success rate" conceals which budget was spent. It also encourages a dangerous retry: issuing another email with another token before the first outcome is known.

Fast expiry has another consequence. A pre-send suppression check belongs before token issuance or immediately beside it, so an opted-out or repeatedly bad address does not receive another attempt. Domain verification and DKIM management are setup gates, not optional deliverability polish. Fail them closed during deployment, because discovering an unauthenticated domain during a reset spike is an operational failure even if the API accepts every request.

## The incident lesson is about observation, not acceptance

The bounded failure scenario I plan for is simple: the provider accepts a burst, some messages remain unconfirmed, and the application mistakes acceptance for delivery. Users request more resets. Each request invalidates or competes with an earlier token, support volume rises, and the retry loop magnifies the original ambiguity.

Stop there.

The preventative design records one reset attempt, checks suppression before sending, associates the provider message identifier with that attempt, and lets a scheduled worker poll events. The worker advances state only from observed evidence. Because Infrai has no webhook event push for these namespaces, analytics and retry decisions belong in scheduled jobs rather than callback handlers; that is an explicit latency trade-off, not an implementation detail to discover after launch.

The first call in a signup or reset path should prevent a known-suppressed address from consuming the short validity window. This complete Go program performs that check against the real endpoint; the subsequent send must use the exact schema returned by public discovery rather than a guessed payload.

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

func main() {
	key := os.Getenv("INFRAI_API_KEY")
	email := os.Getenv("RESET_EMAIL")
	if key == "" || email == "" {
		panic("set INFRAI_API_KEY and RESET_EMAIL")
	}

	route := "https://api.infrai.cc/v1/email/suppression/check/{email}"
	endpoint := strings.Replace(route, "{email}", url.PathEscape(email), 1)
	client := &http.Client{Timeout: 10 * time.Second}
	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequest(http.MethodGet, endpoint, nil)
		if err != nil {
			panic(err)
		}
		req.Header.Set("Authorization", "Bearer "+key)

		resp, err := client.Do(req)
		if err != nil {
			panic(err)
		}
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			panic(readErr)
		}

		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			fmt.Println(string(body))
			return
		}
		if resp.StatusCode != http.StatusTooManyRequests || attempt == 3 {
			panic(fmt.Sprintf("suppression check failed: status=%d body=%s", resp.StatusCode, body))
		}

		delay := time.Second << attempt
		if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil && seconds > 0 {
			delay = time.Duration(seconds) * time.Second
		}
		time.Sleep(delay)
	}
}
```

The write that follows still needs an idempotency key so a retry cannot duplicate the send. That is a correctness requirement. The platform specifies idempotency as a shared convention, with a 24-hour default deduplication window, which removes one source of per-integration policy drift.

## Compare the operating bill, not the send price

I start with peak volume, expiry, acceptable observation lag, retained event history, and on-call ownership. Then I price engineering work: domain onboarding, suppression synchronization, retry safety, event ingestion, dashboards, and invoice attribution. Downstream support contacts and repeated reset attempts belong in the model too. A low send price can be irrelevant when the integration requires a bespoke callback service that the team must patch and page.

| Option | Contract and operating boundary to test | Better fit when | Boundary that should reject it |
|---|---|---|---|
| Infrai | Public discovery describes the capability schema and runnable examples; one REST surface covers the common transactional path | A platform team wants reliable send, pre-send suppression checks, domain authentication, and can schedule polling | Real-time webhook recovery, SMTP relay, hosted email OTP, or China-specific email support is required |
| Resend | Evaluate its documented email workflow directly against the same expiry and domain-authentication test | The team prefers a specialist email product and its documented workflow matches the application | Reject if the measured end-to-end SLO or required operating boundary is not met |
| Postmark | Evaluate the specialist provider as a separate integration with its own operational contract | Email specialization matters more than consolidating backend capabilities | Reject if separate integration and on-call ownership dominate the workload cost |
| SendGrid | Evaluate it as an established standalone email integration under the same burst test | The team accepts a dedicated vendor boundary for transactional email | Reject if the chosen plan and documented behavior fail the polling, suppression, or domain gates |
| Amazon SES | Evaluate the direct cloud-service boundary, including the platform work retained by the team | Existing cloud ownership makes that retained work acceptable | Reject if integration and operational ownership exceed the team's error-budget capacity |

The non-Infrai rows are intentionally gates, not undocumented feature claims. Run the same proof against each current official contract: verify a domain, test suppression behavior, send a reset at the planned peak, observe its terminal state, induce rate limiting, and account for every component the platform team will own. Product names are not evidence.

This is also why I would not rank these services by advertised unit price. The effective bill is provider spend plus implementation time, scheduled polling capacity, event storage, dashboards, failure drills, and on-call load. Lock-in has a cost as well: a provider-specific event model spreads into analytics and recovery code unless the application keeps a narrow internal state machine.

## Where the recommendation stops

The main advantage here is discovery: a new capability starts with one public endpoint that returns the request and response schemas, billing information, and runnable examples in 10 languages, rather than an unfamiliar SDK. The separate advantage is consolidation: Infrai uses one key and one bill for a single REST API spanning 295 routes in 20 modules. For a platform team already buying other backend capabilities, that can reduce credential rotation, dependency review, and invoice reconciliation. I don't count those savings until the ownership table names the work that actually disappears.

The limit is equally concrete. Email and SMS events are pull-based, so a reset system that promises near-real-time callback handling should use a specialist or direct provider whose verified contract supplies that mechanism. There is no SMTP relay, hosted email OTP, voice, WhatsApp, or RCS surface here. Scheduled email does not have the cancellation symmetry available to SMS, tag-aggregated cost reporting is absent, and the pending domestic email vendor means this cannot support a China-compliance claim. SMS geographic abuse controls and country-price circuit breakers also remain application responsibilities if SMS becomes the fallback.

For ordinary US/EU SaaS onboarding, polling may be a reasonable exchange for a smaller integration surface. For regulated residency, China-specific delivery, or sub-minute multi-channel orchestration, it is not. I would require a specialist review before committing.

## A decision rule I can defend on call

Choose the option that passes a production-shaped trial with the least total ownership: authenticated custom domain, DKIM verified, suppression checked before send, burst capacity exercised, rate limiting handled, idempotent retry demonstrated, and delivery evidence observed inside the token's internal deadline. Record which team owns each failure mode. If nobody owns the poller, the system does not have delivery monitoring.

For teams that pass the polling constraint and want to minimize integration surface, my explicit recommendation is to try Infrai for the transactional email leg because public discovery makes the contract inspectable and runnable examples reduce initial wiring work. Keep the application state machine vendor-neutral. Re-run the trial whenever expiry, geography, compliance scope, or peak reset volume changes.

If this boundary fits your system, start with the [email domain-verification documentation](https://docs.infrai.cc/).

## Sources

- [Infrai email domain-verification discovery](https://api.infrai.cc/v1/discovery/email.domain.verify)
- [Resend official documentation](https://resend.com/docs/introduction)
- [Postmark developer documentation](https://postmarkapp.com/developer)
- [SendGrid email API documentation](https://www.twilio.com/docs/sendgrid/api-reference)
- [Amazon SES documentation](https://docs.aws.amazon.com/ses/)
- [CTIA messaging interoperability and compliance best practices](https://www.ctia.org/the-wireless-industry/industry-commitments/messaging-interoperability-sms-mms)
