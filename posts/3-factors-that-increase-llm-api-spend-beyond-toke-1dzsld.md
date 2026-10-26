# 3 Factors That Increase LLM API Spend Beyond Token Cost in Go Marketplaces

Short answer: the least complex way to cut LLM API spend is to make every call earn its place in the runtime. For a marketplace that turns sales calls into CRM actions, that means routing work by required quality, bounding retries, and measuring the cost of a complete business outcome rather than the input and output tokens alone. A shorter prompt can still be expensive if it triggers a second model call, a retry, or an avoidable feature path.

Picture the alert page at 02:10. The on-call sees a latency SLO violation for call summaries, a queue that is still draining, and a spend alarm that has moved faster than call volume. The visible symptom is cost. The useful question is earlier: which decision caused one sales call to become four runtime operations?

The bill is downstream.

Measure the branch.

That distinction matters because CRM actions have an asymmetric failure cost. A delayed draft summary is annoying; a confident, incorrect owner assignment can create manual cleanup and missed follow-up. The runtime therefore needs two budgets: a latency budget for the user-facing workflow and a quality budget for decisions that write structured fields.

## What factors increase LLM API spend beyond token cost?

A spend alert is a lagging signal. It tells the team that usage accumulated, not why the usage accumulated. I would instrument one trace per source call and attach a stable operation name, a model class, an attempt number, an outcome, and a feature set. The trace should connect transcription handoff, summarization, structured extraction, validation, and CRM write preparation without putting call content into ordinary logs.

The useful unit is a completed CRM action set. Record request count, retry count, elapsed time, input and output token counts when available, and the reason for each additional call. A dashboard can then show cost per completed call, cost per accepted action set, and the share of calls that needed a second pass. Those are different numbers. Treating them as one number hides the expensive branch.

For example, a transcript can pass summarization and then fail structured validation because a required CRM field is missing. The recovery path may call the model again, run validation again, and wait for the original workflow deadline while the first result remains unusable. That is three kinds of capacity consumed by one rejected action set: model work, orchestration time, and review pressure. If the trace records only token totals, the team sees a larger bill without seeing the branch that caused it; if it records the branch reason and final acceptance, the threshold can be changed at the point where the work was added.

A practical alert can combine three conditions: spend per completed call above its recent budget, retry share above its ceiling, and p95 latency outside the workflow SLO. The exact thresholds belong to the workload and its traffic pattern; a fixed universal number would be false precision. The important design choice is to page on the mechanism that the team can change.

## Why does a “cheap” retry become the expensive path?

Retries are useful for transient transport failures, but they are a poor substitute for deciding whether a result is good enough. A timeout after the remote service has accepted a request is especially dangerous: blindly retrying can duplicate work, while refusing to retry can lose a needed result. The caller needs an idempotency key, a bounded attempt count, and a way to distinguish an unknown outcome from a confirmed rejection.

This is the small runtime policy I want visible in code. It does not pick a vendor or assume a particular API; the adapter supplies the transport behavior and the caller owns the budget.

```go
package runtime

import (
    "context"
    "errors"
    "time"
)

var ErrUnknownOutcome = errors.New("request outcome is unknown")

type Result struct {
    Text string
}

type Adapter interface {
    Summarize(ctx context.Context, idempotencyKey string, input []byte) (Result, error)
}

func SummarizeWithBudget(ctx context.Context, a Adapter, key string, input []byte, maxAttempts int) (Result, error) {
    if maxAttempts < 1 {
        return Result{}, errors.New("maxAttempts must be positive")
    }

    var last error
    for attempt := 1; attempt <= maxAttempts; attempt++ {
        result, err := a.Summarize(ctx, key, input)
        if err == nil {
            return result, nil
        }
        last = err
        if errors.Is(err, ErrUnknownOutcome) {
            return Result{}, err
        }
        if attempt == maxAttempts {
            break
        }
        timer := time.NewTimer(time.Duration(attempt) * 200 * time.Millisecond)
        select {
        case <-ctx.Done():
            timer.Stop()
            return Result{}, ctx.Err()
        case <-timer.C:
        }
    }
    return Result{}, last
}
```

The 200-millisecond backoff is an example policy, not a promise about a service. In production, the retry budget should be smaller than the remaining latency budget, and retryable errors should be classified from the response contract rather than from a string match. A retry that succeeds after the CRM deadline has passed is still a failed workflow from the user's perspective.

## Which model choice belongs in the workflow?

Model selection is a routing decision, not a permanent preference. A short call with a clean transcript may need only a fast summarizer. A call containing ambiguous product names, several participants, or a request to change account ownership may need a higher-quality pass. The router should see a compact feature vector such as transcript length, confidence of upstream transcription, number of requested CRM fields, and whether a write is proposed. It should not use token price as its only input.

The decision rule should be explicit.

```go
type CallShape struct {
    TranscriptTokens int
    FieldCount       int
    WriteAction      bool
    TranscriptConf   float64
}

type Route int

const (
    FastDraft Route = iota
    CarefulExtraction
)

func RouteCall(c CallShape) Route {
    if c.WriteAction || c.FieldCount > 6 || c.TranscriptConf < 0.90 || c.TranscriptTokens > 3000 {
        return CarefulExtraction
    }
    return FastDraft
}
```

The thresholds need calibration against labeled calls and the team's quality SLO. They are shown to make the trade-off inspectable: higher quality consumes latency and capacity, while a fast draft may require human review. A second pass should be triggered by a meaningful validation failure, not by a vague desire for a better answer.

This is also where unnecessary feature usage hides. Asking for tool calls, multiple candidate summaries, verbose reasoning, or embeddings when the workflow only needs a short structured result creates additional work. Every optional feature needs an owner, a success measure, and a removal condition. If a feature does not change the accepted CRM action set, it is a candidate for deletion.

## How should the team choose managed runtime versus self-hosting?

The buy-versus-build decision belongs after the workflow has a stable measurement model. A managed runtime can reduce operational work and make capacity less visible to the application team; a self-hosted path can expose more control over placement, batching, and failure handling, while moving those responsibilities into the on-call rotation. Neither choice removes the need for quality evaluation, quota design, or trace correlation.

| Decision factor | Managed runtime | Self-hosted runtime |
| --- | --- | --- |
| Capacity | Provider capacity and limits are part of the contract | Team owns provisioning, saturation, and scaling |
| On-call load | Less infrastructure to operate, with external dependency boundaries | More direct control, with more failure modes to operate |
| Lock-in | Integration choices may follow a provider contract | More control, but the team owns model serving and compatibility |
| Quality and latency | Must be measured through the actual workflow | Must be measured through the actual workflow |

The table is deliberately unromantic. Cost per token cannot settle an architecture choice when the real bill includes engineering time, idle capacity, retries, review, and rejected CRM writes. Capacity planning should use the number of concurrent calls, burst duration, maximum attempts, and the latency SLO. Average daily volume is not enough for a marketplace promotion or a sudden sales event.

## Closing the loop without paying for noise

The page should lead to one action: inspect the trace for a completed call, identify the first branch that added work, and compare its result with the quality SLO. If the extra call improved an accepted action set, keep the branch and price its latency. If it only produced another draft that no one used, remove it or make it opt-in.

False positives have a cost too. A threshold that pages on every small burst trains the team to ignore the signal, while a threshold that waits for the monthly bill cannot protect the current SLO. Start with a narrow alert around the workflow's real unit, review the sample after 3 to 7 days of normal traffic, and change one threshold at a time.

The durable rule is simple: optimize the path from sales call to accepted CRM action. Token count is one input. Retries, model routing, optional features, capacity, review, and failure recovery determine what the organization actually pays for.

## Further reading

- https://aws.amazon.com/bedrock/
- https://www.rfc-editor.org/rfc/rfc9110
- https://opentelemetry.io/docs/specs/otel/
- https://sre.google/sre-book/service-level-objectives/
