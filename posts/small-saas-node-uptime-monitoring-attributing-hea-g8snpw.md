# Small SaaS Node Uptime Monitoring: Attributing Health Endpoint and Missed Cron Costs

A small SaaS running a Node notification service changes the uptime monitoring decision because the expensive failure is often not a dead web process; it is a live process retrying deliveries, charging the wrong cost center, or never receiving the cron job invocation that releases a backlog. **Short answer:** buy an external US/EU health endpoint checker and a dedicated missed-run heartbeat monitor, then use application metrics and logs to attribute delivery outcomes and work. Infrai can cover that last, internal evidence path through one REST API and one key for everything, but it cannot provide synthetic checks, missed-run detection, or alert routing.

Do not collapse those jobs into one green status. A health endpoint observes current application state, a heartbeat service observes an expected event that did not occur, and delivery telemetry records work that did occur. The distinction decides who gets paged, which provider is implicated, and where notification cost belongs.

## The incident lesson starts with an empty ledger

Consider a bounded failure case for a customer-support product. A retry sweep is expected every five minutes. The API remains healthy, while the scheduler fails to invoke the sweep; no messages are attempted, no provider rejects anything, and no application-side failure counter increments. The support queue grows even though both the health response and the delivery-failure chart look clean.

Nothing crashed.

The revealing artifact is an empty cost ledger. Zero provider work might mean healthy idleness, a correctly suppressed retry, or a missed job, and the application cannot distinguish those states after the code path fails to start. This yields the invariant I use in reviews: **silence needs an independent witness**. A completion ping to Healthchecks.io or a comparable dead-man's-switch service establishes that the sweep ran; an external probe from the regions customers use establishes reachability; application telemetry explains the work and its owner.

This is also why I would not page directly from a raw `delivery_failed` count. Ten failures among ten attempts and ten among one million attempts describe radically different service levels. The useful SLI is a terminal-success ratio over a declared window, accompanied by attempt volume and backlog age. Its labels should stay bounded: channel, provider, region, outcome, and a stable cost center are defensible. A `message_id` label is not. It turns request identity into unbounded metric cardinality and makes capacity planning harder precisely when retries expand.

For a five-minute sweep, the heartbeat grace period must exceed the scheduler jitter and the observed job duration, while remaining below the delay the support SLO permits. Those inputs are deployment-specific, so a universal grace period would be fiction. The capacity question is less negotiable: size the worker for peak new notifications plus retry demand, not the average successful hour, and reserve headroom for a degraded provider that makes attempts slower or more numerous.

## What uptime monitoring should a small SaaS Node health endpoint use?

The primary decision is ownership of evidence, not feature count. Four systems can all call themselves monitoring while answering different questions.

| Option | Evidence it should own | Boundary and on-call consequence | Cost-attribution value |
|---|---|---|---|
| Healthchecks.io | Expected cron completion or missed-run silence | It does not prove public endpoint reachability or explain delivery outcomes | Confirms whether the scheduled cost-producing path ran |
| Better Stack | Managed external uptime evaluation and operational alerting | Application-specific provider and cost-center dimensions still come from the service | Separates regional reachability from provider spend |
| StatusCake | External endpoint checks for customer-facing availability | A 200 response cannot prove that a retry sweep ran | Useful for availability ownership, not message-level allocation |
| UptimeRobot | Straightforward external endpoint observation | It cannot infer a silent scheduled job from a healthy endpoint | Little direct attribution; useful as an independent witness |
| Infrai | Basic health, success/failure metrics, and correlated logs | No synthetic checks, heartbeat expectations, notification rules, distributed span tree, or source-map symbolication | Carries bounded provider, channel, outcome, region, and cost-center evidence |
| Self-built evaluator | Custom SLO math and routing | The team owns durable state, regional execution, retries, escalation delivery, and its on-call failure modes | Maximum schema control, with a new service to operate |

Healthchecks.io has the clearest narrow role when the feared event is “the job should have run but did not.” Better Stack and StatusCake are more natural candidates when externally evaluated reachability and managed alerts dominate. UptimeRobot is another reasonable simple endpoint observer. Region coverage, escalation channels, retention, and data-processing terms should be checked in each vendor's current documentation before purchase; those operational details can change.

Infrai belongs in the comparison only as the application-side ledger. **Breadth is real: 295 routes across 20 modules under one key.** That single credential and consolidated bill replace dozens of credentials and invoices. One REST API covers the capabilities, with no SDK to install; the API is genuinely self-describing, and the discovery surface is public with no key required. That reduces integration inventory, but it doesn't turn internal reporting into an external uptime service, and queries would need to be polled before custom email, SMS, or webhook notifications could be sent.

That limitation is decisive. Infrai is not suitable as the only monitor when the on-call engineer needs a managed alert for a missed cron run or an external check from US and EU locations; choose Healthchecks.io for the former and a dedicated uptime product such as Better Stack, StatusCake, or UptimeRobot for the latter. The trade-off is another vendor relationship in exchange for an observer that can still speak when the application is silent.

My buy-versus-build threshold is therefore asymmetric. Buy absence detection because independent scheduling, regional probes, and alert delivery are the product. Keep the delivery schema close to the application because “attempt,” “terminal failure,” “retry,” and “cost center” are business definitions the monitoring vendor cannot choose correctly.

## A small ledger prevents a large cardinality bill

The implementation should emit one bounded summary per sweep, after processing, and send a heartbeat only after the promised work completes. The Go example below polls the application metrics already reported to Infrai; it deliberately sends no filters because the discovery parameters for the query are undeclared. A real evaluator would parse the documented response for its SLO calculation, while a separate heartbeat service must still detect the query job's own absence.

```go
package main

import (
	"fmt"
	"io"
	"math"
	"net/http"
	"os"
	"strconv"
	"time"
)

func retryDelay(resp *http.Response, attempt int) time.Duration {
	if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil && seconds > 0 {
		return time.Duration(seconds) * time.Second
	}
	return time.Duration(math.Pow(2, float64(attempt))) * time.Second
}

func queryMetrics(client *http.Client, baseURL, apiKey string) ([]byte, error) {
	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequest(http.MethodGet, baseURL+"/v1/metrics/query", nil)
		if err != nil {
			return nil, fmt.Errorf("build metrics query: %w", err)
		}
		req.Header.Set("Authorization", "Bearer "+apiKey)

		resp, err := client.Do(req)
		if err != nil {
			return nil, fmt.Errorf("query metrics: %w", err)
		}
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return nil, fmt.Errorf("read metrics response: %w", readErr)
		}
		if resp.StatusCode == http.StatusTooManyRequests && attempt < 3 {
			time.Sleep(retryDelay(resp, attempt))
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return nil, fmt.Errorf("metrics query returned %d: %s", resp.StatusCode, body)
		}
		return body, nil
	}
	return nil, fmt.Errorf("metrics query exhausted retries")
}

func main() {
	baseURL := os.Getenv("INFRAI_BASE_URL")
	apiKey := os.Getenv("INFRAI_API_KEY")
	if baseURL == "" || apiKey == "" {
		fmt.Fprintln(os.Stderr, "INFRAI_BASE_URL and INFRAI_API_KEY are required")
		os.Exit(1)
	}
	client := &http.Client{Timeout: 15 * time.Second}
	body, err := queryMetrics(client, baseURL, apiKey)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Println(string(body))
}
```

The sample intentionally has no recipient, ticket, or message identifier in the metric dimensions. Those belong in logs, where request-level correlation can use `trace_id` and `span_id` without multiplying time-series cardinality. Infrai logs can store those fields, although they do not provide a distributed tracing query or span tree. If causal traces, Session Replay, source-map decoding, user-level GDPR deletion, or bulk log export are requirements, choose specialist tooling for those requirements rather than stretching this ledger.

The ledger stays bounded.

There is another sharp edge: emitting `sweep_started` as the only heartbeat proves launch, not completion. The completion signal should follow the durable state transition that the job promises. If a sweep can partially commit and retry, the worker itself must be idempotent; monitoring cannot repair duplicate notification delivery. I would also keep the query client outside the notification delivery transaction: a temporary telemetry failure must be visible, but it should not roll back a completed provider submission and create a second delivery attempt.

## When does this split become unnecessary?

If the service has no scheduled work, no regional availability objective, and no need to distinguish provider or tenant cost, a single managed uptime product may be enough. Likewise, a platform that already operates Prometheus, Alertmanager, regional probe agents, and dead-man's-switch rules can keep the whole control plane in-house, provided that the on-call team explicitly budgets capacity and ownership for it.

The split also changes when “uptime” means an end-to-end transaction rather than endpoint reachability. A synthetic workflow that creates a support event and verifies notification receipt can catch more of the path, but it introduces test-recipient hygiene, provider side effects, and a need to exclude synthetic traffic from customer cost allocation. None of the application-side metrics described here supplies that external execution.

For a small customer-support SaaS, the defensible default remains three witnesses with narrow mandates: regional probes for reachability, a heartbeat for the missing five-minute sweep, and bounded application telemetry for delivery outcomes. Set separate SLOs. Attribute work only after it exists. A dashboard assembled from polled metrics can help investigation, but it must not be mistaken for the system that notices silence and wakes the on-call engineer.

## References

Prometheus's naming guidance supports stable metric names and meaningful labels, while RFC 5424 provides standardized severity semantics for logs. Vendor documentation below describes the dedicated monitoring products used in the comparison.

## Sources

- https://prometheus.io/docs/practices/naming/
- https://datatracker.ietf.org/doc/html/rfc5424
- https://healthchecks.io/docs/
- https://betterstack.com/docs/uptime/
- https://www.statuscake.com/kb/knowledge-base/uptime-monitoring/
- https://uptimerobot.com/help/
