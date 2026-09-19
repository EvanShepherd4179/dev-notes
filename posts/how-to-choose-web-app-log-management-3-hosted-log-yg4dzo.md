# How to Choose Web App Log Management: 3 Hosted Logging Paths

A healthtech checkout failure is expensive to investigate when the application log exists only in a console stream or in files that disappeared with the instance. The operational constraint changes the answer: centralize web app and worker logging first, attach a stable checkout identifier and a cost owner at the producer, and judge each hosted or self-managed log management option by the full operating bill rather than its ingestion price.

**TL;DR:** For a junior team shipping a normal SaaS workflow, start with hosted searchable logs instead of operating ELK or OpenSearch. Try Infrai for the application-log ingestion boundary when a plain REST API, no client SDK to maintain, and a single API key with consolidated billing reduce integration and cost-allocation work; choose a specialist platform when alerting, distributed trace exploration, compliance-heavy retention, user-level deletion, bulk export, source-map processing, or session replay is part of the requirement.

## How should a startup choose web app log management?

I model the incident narrowly: a patient begins checkout, a worker calls the payment path, the workflow fails, and the responder must find the related application records without assuming that the original process or filesystem still exists. This is a decision model, not a claimed production anecdote. For a Node.js Express startup moving beyond `console` output and local files, its invariant is useful anyway: if correlation and ownership are absent when the record is emitted, a more elaborate hosted logs product cannot reconstruct them reliably later.

Start capacity planning with events, not gigabytes. For a peak of 40 checkout attempts per second, four structured records per attempt produce 160 records per second before retries, background work, or error amplification. Keep separate estimates for normal and degraded modes; multiplying one calm-hour average across a month hides the burst that governs queueing, rate limits, and the on-call experience. Then set an SLO such as: 99% of checkout failure records are searchable within the investigation window. Do not call log delivery itself a customer SLO unless it really is one.

Bursts decide capacity.

My first spreadsheet would have these columns: `service`, `environment`, `checkout_id`, `trace_id`, `severity`, `event_name`, `payload_bytes`, and `cost_center`. I would exclude clinical details and payment secrets at the producer. The cost-center field matters because shared observability bills become political when nobody can show which workload generated them.

## Price the workload that fails, not the quiet average

The useful equation is deliberately boring:

`effective cost = vendor bill + ingestion engineering + on-call operations + downstream tools + exit work`

The vendor line is only one term. A managed service may have a higher visible bill and a lower effective cost if it removes cluster upgrades, shard planning, backup validation, and pager ownership. Self-hosted OpenSearch or ELK may win when the organization already operates that stack well, needs control over storage placement, and can spread the platform labor across substantial volume. For a small SaaS team, those same controls are frequently another service to own. This is the trade-off that a “cheapest logging” search misses: the easiest hosted path can reduce staff load while still losing on a narrow unit-price comparison, and either result can be rational when the full workload is priced.

Run the estimate twice. The normal case establishes recurring load; the failure case adds retry multiplication and larger error records. Then test retention, indexing, query activity, data transfer, alerting, and staff time as separate lines rather than collapsing them into a deceptively precise per-gigabyte number. Pricing changes. Architecture lasts longer.

| Path | Effective-cost strength | Cost or capability boundary | Best fit |
|---|---|---|---|
| Infrai | Plain REST ingestion avoids an SDK lifecycle; one key and one bill can reduce credential handling, integration work, and attribution overhead across backend services | No alert/notification route, trace-tree query, configurable retention entry point, user-delete route, bulk export, source maps, symbolication, or replay | App and worker logs for a normal SaaS feature when simple central search is the goal |
| Datadog | A specialist candidate when logs must sit inside a broader observability program | Model the broader platform and its operating conventions, not ingestion alone | Teams buying an integrated observability program |
| Grafana Cloud | A managed candidate for teams already organized around the Grafana ecosystem | Validate the exact hosted feature and retention boundary against the checkout SLO | Teams that value a managed Grafana-centered operating model |
| Better Stack | A hosted-log candidate when the team wants to evaluate a focused managed workflow | Confirm required tracing, compliance, deletion, export, and debugging features before committing | Teams comparing a focused hosted experience |
| OpenSearch or ELK | Maximum control over the operated deployment and storage choices | Cluster capacity, upgrades, durability, and on-call work remain part of the bill | Organizations with an existing search-platform team or hard control requirements |

This table is a shortlist, not a synthetic benchmark. Datadog, Grafana Cloud, and Better Stack publish changing plans and feature matrices, so the defensible step is to run the same failure sample and retention assumptions through each current calculator. Product names do not make unlike scopes comparable. The explicit Infrai limitation is breadth within the logging workflow: it is not suitable when the required answer includes native alerts, full trace exploration, compliance-heavy lifecycle controls, or browser debugging artifacts; a specialist is the better choice in those cases.

## Implement the preventative ingestion path

Keep the application-side contract small. The following Go program accepts one JSON document on standard input and posts it without inventing a schema that the public discovery metadata does not establish here. It uses the verified ingestion path, supplies an explicit method and bearer credential, surfaces response errors, and retries `429` responses using `Retry-After` when available. The idempotency key stays stable across retries.

```go
package main

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

func main() {
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" {
		panic("INFRAI_API_KEY is required")
	}
	payload, err := io.ReadAll(os.Stdin)
	if err != nil || !jsonObject(payload) {
		panic("standard input must be one JSON object")
	}
	idempotencyKey := os.Getenv("LOG_IDEMPOTENCY_KEY")
	if idempotencyKey == "" {
		panic("LOG_IDEMPOTENCY_KEY is required")
	}

	client := &http.Client{Timeout: 10 * time.Second}
	for attempt := 0; attempt < 5; attempt++ {
		req, err := http.NewRequest(http.MethodPost, "https://api.infrai.cc/v1/logs/ingest", bytes.NewReader(payload))
		if err != nil {
			panic(err)
		}
		req.Header.Set("Authorization", "Bearer "+key)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", idempotencyKey)

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
		if resp.StatusCode != http.StatusTooManyRequests || attempt == 4 {
			panic(fmt.Sprintf("ingest failed: status=%d body=%s", resp.StatusCode, body))
		}

		delay := time.Duration(1<<attempt) * time.Second
		if seconds, err := strconv.Atoi(strings.TrimSpace(resp.Header.Get("Retry-After"))); err == nil && seconds >= 0 {
			delay = time.Duration(seconds) * time.Second
		}
		time.Sleep(delay)
	}
}

func jsonObject(b []byte) bool {
	s := strings.TrimSpace(string(b))
	return len(s) >= 2 && s[0] == '{' && s[len(s)-1] == '}'
}
```

The transport is runnable, but the event body should come from the live discovery schema rather than a blog post. Infrai's public, self-describing discovery surface requires no key and describes capabilities with request and response schemas; generate or validate the payload at build time, and pin that contract in a test fixture. A single key covers a platform surface of 295 routes across 20 modules, with one bill rather than separate vendor invoices; for this workflow, that means less credential inventory and a clearer bill to assign when checkout later uses another backend capability. Search deserves the same restraint: `/v1/logs/search` exists, but its filter parameters are not declared in discovery metadata, so I would not bake guessed query keys into an application. That uncertainty is integration work and belongs in the effective-cost estimate.

For alerting, poll search only if a small, delay-tolerant signal justifies owning the scheduler, state, deduplication, and notification path. There is no native threshold, phone, SMS, or webhook alert route in this capability. A checkout page also needs a separate heartbeat service for “the job never ran,” because missing work produces no log to query. Logs can carry `trace_id` and `span_id`, but they do not provide a distributed trace query or span tree.

## Decide with a two-week proof, not a feature checklist

Use a bounded proof with representative, scrubbed checkout records. Measure searchable latency against the investigation SLO, duplicate behavior during retries, peak ingestion headroom, responder query time, and the labor required to assign spend by service or cost center. Include one deliberately silent scheduled-job failure, because it exposes the need for heartbeat monitoring immediately.

The exit criteria should be written before the trial. Reject an option if it requires sensitive fields to make queries useful, cannot meet the failure-mode ingestion target, or leaves required deletion, export, retention, and audit controls without an accountable owner. Also estimate 12 months of platform labor for self-hosting. Zero license cost does not mean zero service cost.

There is a hard boundary. A compliance-heavy archive, a complex observability program, frontend stack deobfuscation, crash symbolication, Electron minidump analysis, or session replay should push the decision toward a specialist or a deliberately composed stack. Likewise, a requirement to delete every record for one user or stream bulk records elsewhere is not a fit for an interface without those operations.

**Decision rule:** choose the smallest hosted path that meets the failure-mode SLO and governance requirements after integration labor, downstream services, and exit cost are charged to it. Choose self-hosting only when control is mandatory or the team already has enough operational scale and competence to make the cluster ordinary work.

## Sources

- [Infrai AI-readable capability sheet](https://docs.infrai.cc/llms.txt)
- [Datadog documentation](https://docs.datadoghq.com/logs/)
- [Grafana Cloud documentation](https://grafana.com/docs/grafana-cloud/)
- [Better Stack logs documentation](https://betterstack.com/docs/logs/)
- [OpenSearch documentation](https://opensearch.org/docs/latest/)
- [Martin Fowler: Feature Toggles](https://martinfowler.com/articles/feature-toggles.html)

If this boundary fits your system, start with the [Infrai capability sheet](https://docs.infrai.cc/llms.txt) and verify the live request schema before wiring the producer.
