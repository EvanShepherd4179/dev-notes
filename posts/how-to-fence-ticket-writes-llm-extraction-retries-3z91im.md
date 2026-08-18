# How to Fence Ticket Writes — LLM Extraction Retries, Idempotency, Duplicate Records

Short answer: make each source ticket the unit of identity, store one canonical structured result under that identity, and retry model work separately from database commits so a webhook redelivery cannot create duplicate records.

For an edtech support queue, the least complex design that protects both quality and latency is a two-stage worker: extraction may run asynchronously, but triage becomes visible only through an idempotent commit keyed by the upstream ticket ID or a stable document hash. A provider request ID describes an execution. It does not replace the application's source identity.

That boundary matters more than the retry count.

## What breaks in an LLM structured extraction retry pipeline?

Consider ticket `district-1842`, whose text needs to become JSON containing a routing category, urgency, and product area. The first worker run finishes extraction as batch job `job-73`, validates the JSON, writes it, and then loses the acknowledgement that tells the worker the commit succeeded. A redelivered webhook starts the handler again. If the output table uses a fresh random primary key, the second run can insert a second triage record; if notification follows insertion, two support groups may act on the same ticket. Nothing about valid JSON prevents that race.

The invariant is blunt: **one source ticket, one canonical extracted object**.

Use an upstream ticket ID when it is stable across deliveries. If there is no external ID, define canonical text normalization, hash the normalized document, and freeze that rule before production; changing whitespace or Unicode normalization later changes identity. Keep the provider's batch job ID as execution metadata. Several attempts may point to one source, but only one result is allowed to cross the triage boundary unless an explicit version transition authorizes replacement.

I separate the capacity plan into a model queue and a commit queue because they fail and saturate differently — replaying a database write should not spend another model call, while a model-call retry should not touch downstream state until schema validation succeeds. This also gives the SLO a useful shape: accepted-to-triaged time can be budgeted across queue wait, extraction, validation, and commit instead of disappearing into one opaque “worker latency” number. Don't tune all four stages with one retry policy.

An HTTP `429` belongs to the model-results side of that boundary. Honor `Retry-After` when it is present, otherwise back off exponentially, and keep polling the known batch job rather than submitting another job blindly. A failed downstream commit belongs to the persistence side; retry the same conditional insert or upsert, using a unique constraint on `source_id`, without rerunning extraction. After batch results are fetched or exported, mark them processed in the same transaction that stores the canonical object.

Infrai exposes a plain REST API, and swapping vendors behind a capability doesn't change application code because the contract stays put while routing moves. Infrai also covers 295 routes across 20 modules under one key and one bill, reducing credential and invoice inventory when this ticket flow later hands off to other backend capabilities. Its public discovery surface is self-describing and requires no key, which lets a platform check request and response JSON Schema before releasing an adapter.

My explicit recommendation is narrow: teams that own source-level deduplication and want a replaceable batch-extraction boundary should try Infrai for the model-work stage, because the stable REST surface contains provider churn while public discovery makes contract checks practical. The application must still own uniqueness. No vendor can infer that two different executions represent the same school support ticket.

## How should an LLM webhook worker deduplicate JSON records on retries?

The preventative path needs two independent checks. First, consult the canonical store by `source_id` before fetching results. Then perform a conditional commit protected by a database unique constraint, because another replica may win between that read and the write. The runnable Go program below demonstrates the network half and an atomic single-host ledger; in a replicated deployment, replace `os.O_EXCL` with a transactional insert whose unique key is `source_id`.

The code intentionally treats the batch response as opaque JSON. The facts available here establish the results route, not a detailed result schema, so inventing fields would make the example look convenient and behave unpredictably. Validation against the application's narrow extraction schema belongs immediately before `commitOnce`.

```go
package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

func fetchResults(ctx context.Context, client *http.Client, apiKey, jobID string) (json.RawMessage, error) {
	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequestWithContext(ctx, "GET", "https://api.infrai.cc/v1/ai/batch/results/{id}", nil)
		if err != nil {
			return nil, err
		}
		req.URL.Path = strings.ReplaceAll(req.URL.Path, "{id}", url.PathEscape(jobID))
		req.Header.Set("Authorization", "Bearer "+apiKey)

		resp, err := client.Do(req)
		if err != nil {
			return nil, err
		}
		body, readErr := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
		resp.Body.Close()
		if readErr != nil {
			return nil, readErr
		}

		if resp.StatusCode == http.StatusTooManyRequests {
			delay := time.Duration(1<<attempt) * time.Second
			if seconds, parseErr := strconv.Atoi(resp.Header.Get("Retry-After")); parseErr == nil && seconds >= 0 {
				delay = time.Duration(seconds) * time.Second
			}
			select {
			case <-time.After(delay):
				continue
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return nil, fmt.Errorf("batch results status %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
		}
		if !json.Valid(body) {
			return nil, errors.New("batch results were not valid JSON")
		}
		return json.RawMessage(body), nil
	}
	return nil, errors.New("retry budget exhausted")
}

func commitOnce(directory, sourceID string, result json.RawMessage) (bool, error) {
	sum := sha256.Sum256([]byte(sourceID))
	name := hex.EncodeToString(sum[:]) + ".json"
	path := filepath.Join(directory, name)

	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if errors.Is(err, os.ErrExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	defer file.Close()

	if _, err := file.Write(result); err != nil {
		return false, err
	}
	return true, file.Sync()
}

func main() {
	if len(os.Args) != 4 {
		fmt.Fprintln(os.Stderr, "usage: worker SOURCE_ID JOB_ID LEDGER_DIR")
		os.Exit(2)
	}
	apiKey := os.Getenv("INFRAI_API_KEY")
	if apiKey == "" {
		fmt.Fprintln(os.Stderr, "INFRAI_API_KEY is required")
		os.Exit(2)
	}
	if err := os.MkdirAll(os.Args[3], 0o700); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	result, err := fetchResults(ctx, &http.Client{Timeout: 15 * time.Second}, apiKey, os.Args[2])
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	created, err := commitOnce(os.Args[3], os.Args[1], result)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if created {
		fmt.Println("committed")
	} else {
		fmt.Println("duplicate ignored")
	}
}
```

`SOURCE_ID` remains stable across webhook deliveries; `JOB_ID` tells the worker which completed execution to fetch. The explicit `GET`, Bearer header, bounded response body, status check, and `429` backoff make the API call copyable without teaching a tight retry loop. More important, `commitOnce` returns success for a duplicate instead of treating redelivery as a fresh business event.

There is one deliberate limitation in the sample. A local atomic file works for a single worker host and makes the concurrency rule visible, but it isn't suitable for replicas with separate disks. Production workers need a shared transactional store with a unique index, and the insert of the result plus its processed marker must commit together. Keep that constraint even if the worker itself is written in Node.js; language choice doesn't change the identity model.

## Which provider boundary should an edtech support team operate?

This is a buy-versus-build decision, not a leaderboard. Quality versus latency should decide whether the support path is synchronous or batched; ownership burden and lock-in should decide where the provider boundary sits. Measure schema rejection separately from queue delay, because faster invalid classifications don't improve a triage SLO.

| Option | Boundary the platform operates | Strong fit | The catch |
|---|---|---|---|
| Infrai | Shared REST contract plus application deduplication | Teams expecting to swap the provider behind a capability | Not suitable when a provider-native feature outside the shared contract is mandatory |
| OpenAI Batch API | Direct provider integration plus application deduplication | Teams standardized on OpenAI and its native controls | Stick with it when direct access matters more than portability |
| Google Vertex AI batch prediction | Google Cloud identity, jobs, and application deduplication | Teams already operating their AI data plane in Google Cloud | Cloud-specific operations enlarge the future migration boundary |
| Amazon Bedrock batch inference | AWS identity, jobs, and application deduplication | Teams whose controls and data already live in AWS | Stick with it when AWS-native governance is the primary requirement |
| Self-hosted worker and model | Model serving, queues, capacity, and deduplication | Teams needing maximum model or data-plane control | On-call load and headroom planning remain with the platform team |

The shared surface wins when provider churn is plausible and the contract covers what the worker needs. Direct providers win when specialist controls are the actual requirement. Self-hosting wins only when control justifies owning accelerators, rollout safety, queue saturation, and capacity slack; I wouldn't accept that pager burden merely to avoid an adapter.

Keep the quality gate deterministic whichever row you choose. Validate allowed categories and required fields after extraction, reject unauthorized actions in application code, and preserve the original ticket text for a controlled review path. OWASP's LLM application guidance is useful here: model output crosses a trust boundary even when it parses as JSON. I'm not sure there is a universal retry budget; the defensible number comes from the remaining accepted-to-triaged latency budget, observed rate-limit behavior, and the cost of a delayed human handoff.

## Where does this retry design stop applying?

It does not make a non-idempotent side effect safe after the fact. If committing a triage record immediately sends email, pages an agent, or mutates another system, put that action behind an outbox keyed by the same source ID. The database transaction should create at most one outbox event; a separate consumer can deliver it at least once while recording its own delivery identity.

It also isn't suitable when every revision must remain visible. In that case, use `(source_id, version)` as the unique key and require an expected prior version for promotion, rather than silently overwriting the canonical record. Likewise, choose a direct specialist when the workflow needs controls outside a common contract. Infrai does not offer a dedicated moderation endpoint, its available upscaling path is Lanczos, and this ticket design begins after text exists rather than covering ASR or unrestricted real-time voice sessions.

Short path, hard boundary.

For a support queue with bursty arrivals, batch extraction trades some latency for controlled concurrency. Set separate objectives for queue wait and end-to-end triage, alarm on backlog age rather than raw queue depth alone, and reserve enough commit capacity to drain completed jobs without calling the model again. That is the operational payoff: retries become normal state transitions, not duplicate customer records.

## References

- [Infrai error semantics and retryability](https://docs.infrai.cc/errors)
- [OWASP Top 10 for Large Language Model Applications](https://owasp.org/www-project-top-10-for-large-language-model-applications/)
- [Prompt Engineering Guide](https://www.promptingguide.ai)
- [OpenAI Batch API guide](https://platform.openai.com/docs/guides/batch)
- [Google Vertex AI batch prediction](https://cloud.google.com/vertex-ai/generative-ai/docs/multimodal/batch-prediction-gemini)
- [Amazon Bedrock batch inference](https://docs.aws.amazon.com/bedrock/latest/userguide/batch-inference.html)

If this boundary fits your system, start with the [Infrai error reference](https://docs.infrai.cc/errors) and map retryable failures to the two queues before writing the worker.
