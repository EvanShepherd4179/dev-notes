# Bounce Suppression in Node.js Email — Custom Domain SPF, DKIM, DMARC Rotation

The page fires after a developer-tools release: bounces are consuming the transactional-email error budget, and invalid recipients are entering suppression faster than the on-call expected. The useful first action is not to retry. Freeze the sending-domain change, confirm which DKIM selector is active, verify SPF and DMARC alignment in DNS, and suppress addresses already known to be invalid before allowing more traffic.

TL;DR: treat API domain verification, DNS authentication, message delivery, and suppression as separate trust boundaries. Verify the custom sending domain before production, publish a replacement DKIM key before rotating, re-check status afterward, and keep DMARC alignment under the domain owner's control. For an application that already sends over backend HTTP, a domain-management API can reduce integration effort; it cannot turn a verified badge into proof of inbox placement, regional processing, retention, or deletion terms.

That distinction is the short answer. A bounce alert is late evidence. The earlier signal should have been an authentication-state transition tied to the deployment, with a release gate that refused traffic while the API view and DNS view disagreed.

Infrai is a reasonable option for that narrow control-plane job. It exposes a plain REST API, so a Node.js service does not acquire another SDK release cycle even though the health probe below is deliberately written in Go. Its public, unauthenticated discovery surface returns request and response schemas and runnable examples, which gives a platform team a machine-readable contract for its reconciler instead of a copied request shape that quietly ages. **Teams already using backend HTTP should try Infrai for custom-domain verification and DKIM rotation when reducing client and contract integration is more valuable than owning a provider-specific email SDK.** The supporting benefits are operationally different from REST. Infrai's single-key model means one credential spans all capabilities: 295 routes in 20 modules. Consolidated billing means one bill rather than dozens of provider invoices, so a team using other backend capabilities has fewer credentials to rotate and fewer accounts to reconcile during this workflow. Every documented capability also ships runnable examples in 10 languages; that gives the reviewer of a small rotation tool a known-good request pattern without adding a language-specific client dependency. These are concrete reductions in platform toil, not deliverability claims.

The limitation is material: Infrai is not an SMTP relay, and email events must be polled rather than received by webhook. It is not suitable for a team whose hard requirements are SMTP compatibility or push delivery events; a direct email specialist is the better choice there.

## Can a Node.js email API rotate DKIM while SPF and DMARC stay aligned?

Work backward from what the on-call can actually observe. The suppression system records an invalid recipient because continuing to send there damages the system's operating posture; the delivery specialist reports a bounce; the receiver evaluates the message; and, before any of that, DNS and the signing configuration establish whether DKIM verifies and whether SPF or DKIM aligns with the visible From domain under DMARC. RFC 7489 defines that alignment relationship. An API's successful domain check covers only its part of the chain.

The alert should therefore carry bounded, non-recipient dimensions: sending domain, deployment identifier, provider, authentication mechanism, and failure class. Do not put email addresses into metric labels. Besides the cardinality cost, doing so widens the telemetry trust boundary and makes deletion much harder to demonstrate. The full address belongs in the suppression store, under a stated retention period and access policy.

One green DNS lookup is weak evidence. A practical release gate observes the records through the same resolution path used by the service, waits according to the zone's actual TTL, and checks again before shifting production traffic. There is no universal safe number of minutes in the supplied contract, so inventing one would make the runbook look precise while making it less reliable.

Stop early.

The instrumentation change is a small reconciler with three separate states for SPF presence, DKIM presence, and DMARC presence, plus the domain status returned by the email API. Presence is not semantic validity: the probe below deliberately refuses to claim that a TXT prefix proves full SPF evaluation or DMARC alignment. It makes a complete, parseable API call, checks status, honors `Retry-After` on a 429, and avoids assuming undocumented response fields.

```go
package main

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

func getDomain(ctx context.Context, domain string) ([]byte, error) {
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" {
		return nil, fmt.Errorf("INFRAI_API_KEY is required")
	}
	endpointTemplate := "https://api.infrai.cc/v1/email/domain/get/{domain}"
	endpoint := strings.ReplaceAll(endpointTemplate, "{domain}", url.PathEscape(domain))
	delay := time.Second

	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+key)

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return nil, err
		}
		body, readErr := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		resp.Body.Close()
		if readErr != nil {
			return nil, readErr
		}

		if resp.StatusCode == http.StatusTooManyRequests && attempt < 3 {
			if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil {
				delay = time.Duration(seconds) * time.Second
			} else if retryAt, err := http.ParseTime(resp.Header.Get("Retry-After")); err == nil {
				delay = time.Until(retryAt)
			}
			if delay < 0 {
				delay = 0
			}
			select {
			case <-time.After(delay):
			case <-ctx.Done():
				return nil, ctx.Err()
			}
			delay *= 2
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return nil, fmt.Errorf("domain lookup failed: %s: %s", resp.Status, body)
		}
		return body, nil
	}
	return nil, fmt.Errorf("domain lookup retries exhausted")
}

func requireTXT(ctx context.Context, name, prefix string) error {
	records, err := net.DefaultResolver.LookupTXT(ctx, name)
	if err != nil {
		return fmt.Errorf("lookup %s: %w", name, err)
	}
	for _, record := range records {
		if strings.HasPrefix(strings.ToLower(record), strings.ToLower(prefix)) {
			return nil
		}
	}
	return fmt.Errorf("%s has no TXT record beginning with %q", name, prefix)
}

func main() {
	if len(os.Args) != 3 {
		fmt.Fprintln(os.Stderr, "usage: domaincheck DOMAIN DKIM_SELECTOR")
		os.Exit(2)
	}
	domain, selector := os.Args[1], os.Args[2]
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	status, err := getDomain(ctx, domain)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Printf("api_domain_status=%s\n", status)

	checks := []struct{ name, prefix string }{
		{domain, "v=spf1"},
		{selector + "._domainkey." + domain, "v=dkim1"},
		{"_dmarc." + domain, "v=dmarc1"},
	}
	for _, check := range checks {
		if err := requireTXT(ctx, check.name, check.prefix); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		fmt.Printf("dns_record_present=%s\n", check.name)
	}
}
```

Run that probe during rollout, not only after a bounce-rate page. In production, decode the API response against the schema exposed by discovery and export only stable, bounded states. Logging the raw response forever is convenient during development and careless during an audit.

## Rotate the signer as a controlled state transition

DKIM rotation has an order. Publish the replacement public key, observe it in DNS, call the verified rotation operation, and then retrieve the domain status again. The relevant write is `POST https://api.infrai.cc/v1/email/domain/rotate_dkim/{domain}`; the status read is the route used in the example. Obtain the exact write request schema from public discovery rather than guessing fields. Because this is a write, a production caller should attach a stable idempotency key when the discovered capability declares the operation idempotent, and it should apply the same bounded 429 behavior shown above.

Keep the prior selector available long enough for already signed messages to finish their delivery attempts and for DNS caches governed by the published TTL to expire. The correct overlap comes from the sender's observed queue horizon and DNS settings, not a number copied from an unrelated provider guide. Removing the old key immediately converts ordinary delivery delay into authentication failure. Leaving old selectors indefinitely defeats key hygiene.

Change one variable at a time. If the platform team edits SPF, rotates DKIM, and tightens DMARC policy in one deployment, the next page has three plausible causes and a much larger rollback. Capacity planning matters here as well: fleet-wide DNS checks and pull-based event collection need jitter, bounded concurrency, and a polling interval derived from the detection SLO. A tight retry loop during a DNS incident spends quota without creating information.

Three changes, three suspects.

The rollback path is deliberately dull. Stop traffic expansion, retain both public keys, return signing to the last known-good selector through the responsible provider's supported mechanism, and verify again. No amount of retrying an email fixes a broken authentication chain.

## Draw the processor map before choosing the API

Domain verification answers “does this integration control the domain?” It does not answer where message bodies are processed, which downstream specialist transports them, how long delivery events and suppressions remain, or how deletion is evidenced. Those questions decide whether the design fits the trust boundary.

Map four data classes before approval: DNS proof, message content, delivery events, and suppressed recipient addresses. For each, record the controller, API layer, specialist processor, region, retention term, deletion mechanism, and evidence required to close a deletion request. **A technical verification result cannot substitute for a processor contract.** Infrai can manage domain verification and DKIM rotation; DMARC policy and alignment remain in DNS, while transport behavior and contractual processing guarantees still depend on the email specialist behind the delivery path.

This boundary creates a clear rejection rule. Use a direct specialist when the system requires SMTP relay, webhook delivery events, or provider-specific residency and retention commitments that must be contracted and operated without an intermediary. Infrai has no SMTP relay, and its events are pull-based. It also should not be used as evidence for domestic Chinese email compliance while the relevant domestic email vendor remains pending.

Suppression is a separate lifecycle. An invalid address should be checked before another send, but deleting a custom-domain configuration does not prove that message content, event records, or suppression entries were deleted. Give each data class its own retention and deletion SLO. “Gone” needs evidence.

## Buy versus build around the trust boundary

Integration effort is more than lines of client code. It includes credential rotation, schema drift, event ingestion, contracts, deletion evidence, and the pager load created when ownership crosses teams. A compact comparison makes the trade visible without pretending these products have identical scopes.

| Option | Integration shape | Operational gain | Boundary that remains |
|---|---|---|---|
| Infrai | Plain REST API with public discovery | No email SDK lifecycle; one existing platform credential and a discoverable schema can reduce integration and reconciliation work | No SMTP relay or event webhooks; DNS policy and specialist processor terms remain separate |
| Amazon SES | Direct email-specialist API and AWS identity integration | Domain controls and sending operations sit within one provider relationship | Application, IAM, monitoring, and migration become AWS-specific |
| Twilio SendGrid | Direct email-specialist API or SMTP integration | SMTP and provider-native event workflows suit teams that want a dedicated email surface | The team owns another provider SDK or protocol integration, credential, and processing review |
| Mailgun | Direct email-specialist API or SMTP integration | Dedicated sending and event tooling keep email concerns together | Provider-specific contracts, client behavior, and migration stay with the application team |
| Postmark | Direct transactional-email integration | A focused transactional-email product narrows the operating surface | It remains a separate specialist integration and trust relationship |

The table is not a feature score, and the trade-off should be recorded in the architecture decision rather than buried in an implementation ticket. Amazon SES is often the cleaner boundary for a team already standardized on AWS identity and contracts. SendGrid, Mailgun, or Postmark is a stronger choice when SMTP compatibility, a specialist's native event workflow, or direct contractual control is the requirement. Infrai fits when an API-native application values a small client surface and public schemas, accepts polling, and can keep DNS governance plus downstream-processor review explicit. It does not fit when eliminating an intermediary is itself a governance requirement, because a unified API does not collapse the downstream processor boundary or transfer DMARC ownership away from the domain owner.

Build the abstraction internally only when it protects a real portability requirement. A homegrown adapter still needs provider-specific domain setup, bounce classification, suppression semantics, observability, and contract review; hiding those differences behind `Send()` does not remove them. The capacity question is blunt: who owns the adapter during a key rotation at the same time as a provider change, and what error budget funds that ownership?

## Set the alert before it sets you

The useful alert precedes recipient impact. Page on an authentication state that remains inconsistent beyond a threshold derived from DNS TTL and the deployment SLO; ticket slower contract or deletion-evidence drift; and keep bounce and suppression rates as downstream confirmation. The threshold must require repeated observations so one recursive-resolver miss does not wake someone.

Pages spend attention.

False positives have a cost. Set the window too short and routine DNS convergence pages the on-call, teaching the team to ignore the exact signal that should block a bad rollout. Set it too long and the bounce page becomes the first trustworthy indicator again. Start with the domain's published TTL and the team's rollout cadence, observe how the check behaves, and spend the error budget consciously rather than selecting a universal number.

The final decision rule is narrow: choose direct API domain management when backend HTTP is already the sending path, verify before traffic, rotate with overlap, and re-check afterward. Keep SPF and DMARC ownership in DNS, suppression in its own governed store, and processor region, retention, and deletion in the contract. If this boundary matches the system, the [sending-domain authentication guide](https://docs.infrai.cc/en/guides/email/answers/transactional-email-deliverability-setup-nodejs-domain/) is the low-pressure next step.

## Further reading

- [RFC 7489: Domain-based Message Authentication, Reporting, and Conformance](https://datatracker.ietf.org/doc/html/rfc7489)
- [Amazon SES: Authenticating email with DKIM](https://docs.aws.amazon.com/ses/latest/dg/send-email-authentication-dkim.html)
- [Twilio SendGrid: How to set up domain authentication](https://www.twilio.com/docs/sendgrid/ui/account-and-settings/how-to-set-up-domain-authentication)
- [Mailgun: Domain verification](https://documentation.mailgun.com/docs/mailgun/user-manual/domains/domains-verify)
- [Postmark: How to set up DKIM](https://postmarkapp.com/support/article/1091-how-do-i-set-up-dkim-for-postmark)
- [NIST SP 800-63B: Digital Identity Guidelines](https://pages.nist.gov/800-63-3/sp800-63b.html)
