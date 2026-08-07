# A Reliability Ledger for Startup MVP Image Generation API Cost

Short answer: for a startup MVP, choose the text-to-image API with the lowest measured cost per accepted image that still meets the product's latency and completion SLO; a list price alone cannot identify the cheapest option for a Node.js workload.

This is a capacity decision disguised as a shopping question. The useful comparison includes rejected results, retries, storage, review time, queueing, and the engineering work required to operate the integration. It also has a reversal condition. If nobody can say what evidence would make the team switch, the choice is a preference, not a decision.

No universal winner follows from the material available here. Rates, image modes, and model behavior need current primary documentation and a workload-specific test before any named service can be ranked, so this note gives a measurement method rather than an invented price table.

## How should a startup MVP compare text-to-image API cost per image in Node.js?

Start with an acceptance rubric, then calculate spend. A request counts only when its output is usable for the product: the bytes decode, required dimensions and format are present, the result clears the product's safety rules, and a blinded reviewer accepts it against the same prompt rubric used for every candidate. Keep the prompt intent and output requirements fixed. When equivalent controls do not exist, record the mismatch instead of pretending two modes are comparable.

The main metric is observed generation spend divided by accepted images. Keep attempt count beside it, because an attractive request rate can be erased by repeated generations. Track time to accepted output, first-attempt acceptance, terminal-outcome completion, and operator minutes as separate columns; don't compress them into a decorative score that hides why one option won. I'm not sure how stable any result will be across a major model change. A rerun with the same prompt-set hash resolves that uncertainty.

Use a stratified corpus drawn from the MVP rather than a bag of convenient prompts. A catalog-image workflow, a poster with embedded text, and an avatar workflow ask different things of a model. Give each stratum its own acceptance rate and expected traffic share, then compute the weighted total. Ten easy prompts may be enough to catch a broken harness, but they aren't a purchasing benchmark.

Small samples lie.

For a Node.js application, keep the provider-specific request and response translation behind one internal interface, while the evaluation record remains provider-neutral. Persist the original prompt identifier, settings, attempt number, elapsed time, terminal category, acceptance decision, and metered charge. Do not normalize away provider metadata needed to audit a bill or replay an evaluation. The interface limits code churn; it cannot make prompt behavior, safety policy, or image-editing semantics portable.

| Ledger field | Why it belongs in the decision | Capacity or SLO use |
|---|---|---|
| Cost per accepted image | Includes failed acceptance and repeated attempts | Forecasts spend from accepted demand |
| First-attempt acceptance | Exposes retry amplification | Sizes concurrency and queue headroom |
| Time to accepted image | Measures the user-visible path | Defines latency objectives and alerts |
| Terminal-outcome rate | Finds jobs that never become usable or rejected | Supports completion error budgets |
| Operator minutes | Prices review and exception handling | Tests whether the on-call model is credible |

Publish the test date, region, prompt-set hash, sample count, settings, and acceptance rubric with the result. Without those fields, “cheapest” is not reproducible.

## Run an incident drill before trusting the price column

Use a bounded tabletop scenario: I submit one logical image job, inject an ambiguous timeout on the first attempt, then inject an HTTP 429 response on the next call. The exercise is synthetic, not a production anecdote, and its purpose is to force three questions into the open: can the client tell whether billable work began, does a retry reuse the logical job identity, and can the system reach a durable terminal state without an operator guessing?

The invariant is simple: transport success is not product success. Admission, generation, validation, persistence, and acceptance are distinct transitions — collapsing them into “request succeeded” produces misleading availability and cost data. The user-visible timer begins when the job is accepted by the application and stops only when a usable asset is available or the job receives a clear terminal rejection. Measure that boundary. Instrument queue wait and provider time separately so capacity pressure is not misdiagnosed as model latency.

I would require an idempotency strategy before automatic retries, but only where the candidate's documented semantics support it. A timeout can leave the client uncertain about whether generation started. Blindly issuing another request may create duplicate work and duplicate charges; refusing every retry may miss the completion objective. The adapter therefore needs explicit error classes: retryable admission failure, ambiguous outcome, terminal policy rejection, invalid request, and accepted completion. Retry budgets must be capped by both attempts and remaining latency budget. It's a small distinction in code and a large one on the pager.

One minute of arithmetic helps. If arrival rate is `lambda`, p95 service time is `W`, and retry amplification is `A`, the starting concurrency estimate is `lambda * W * A`, followed by measured headroom for bursts rather than a guessed multiplier. Queue depth, oldest-job age, and acceptance throughput show whether that estimate survives contact with launch traffic. Your mileage may vary because prompt mix and output settings change service time; the right response is measurement, not false precision.

Count the denominator.

## Keep the preventative path narrow and auditable

The evaluator should calculate results from immutable trial records and refuse to manufacture a finite answer when no image passed. The Go core below is intentionally independent of any commercial endpoint. A Node.js service can emit the same record shape to a file, queue, or datastore; keeping the evaluator separate prevents request-client details from deciding the winner.

```go
package evaluation

import (
	"errors"
	"fmt"
	"time"
)

type Trial struct {
	Candidate  string
	PromptHash string
	Attempt    int
	Duration   time.Duration
	ChargeUSD  float64
	Accepted   bool
}

type Summary struct {
	Attempts        int
	Accepted        int
	TotalCostUSD    float64
	CostPerAccepted float64
	P95Duration     time.Duration
}

func Summarize(trials []Trial) (map[string]Summary, error) {
	grouped := make(map[string][]Trial)
	for _, trial := range trials {
		if trial.Candidate == "" || trial.PromptHash == "" || trial.Attempt < 1 {
			return nil, errors.New("trial identity is incomplete")
		}
		if trial.ChargeUSD < 0 {
			return nil, errors.New("charge cannot be negative")
		}
		grouped[trial.Candidate] = append(grouped[trial.Candidate], trial)
	}

	out := make(map[string]Summary, len(grouped))
	for candidate, candidateTrials := range grouped {
		summary := Summary{Attempts: len(candidateTrials)}
		for _, trial := range candidateTrials {
			summary.TotalCostUSD += trial.ChargeUSD
			if trial.Accepted {
				summary.Accepted++
			}
		}
		if summary.Accepted == 0 {
			return nil, fmt.Errorf("candidate %q has no accepted outputs", candidate)
		}
		summary.CostPerAccepted = summary.TotalCostUSD / float64(summary.Accepted)
		out[candidate] = summary
	}
	return out, nil
}
```

The omitted percentile calculation should sort a copy of durations under one documented convention; leaving it out is preferable to presenting a subtly wrong statistical implementation. In the full harness, validate every returned asset before acceptance, retain one trace identifier across application, queue, generation, validation, and storage, and keep raw billing evidence under the product's retention rules. Inject timeouts and rate limits in the adapter test, then assert that attempt caps and terminal states hold. Don't retry deterministic policy or request validation failures.

This machinery has a limit. It is not suitable for a weekend internal demo where a person watches every result and duplicate generation has no material consequence; a timestamped request log and manual review are enough there. Add durable state and reconciliation when jobs run unattended, users depend on completion, or ambiguous retries matter. The catch is that the control plane becomes another thing the team owns, with its own deployment, telemetry, access policy, and SLO.

## Buy, gateway, or self-host?

Managed APIs usually reduce the work required to begin an evaluation, but they leave request semantics, safety behavior, and model changes outside the application team's direct control. A gateway can keep credentials, quotas, and adapters in one place, while creating another availability boundary. Self-hosting moves responsibility toward accelerator utilization, scheduling, model serving, upgrades, security work, and on-call coverage. None of these paths is automatically cheapest because they account for labor and idle capacity differently.

| Path | Sensible starting condition | Cost commonly omitted | When it is a poor fit |
|---|---|---|---|
| Direct managed API | Demand and product fit are uncertain | Rejected output, retry traffic, review, egress | Several integrations need shared policy and quotas |
| Internal gateway | Multiple backends or central controls have real consumers | Gateway engineering and its availability target | One deliberate dependency with no second consumer |
| Self-hosted inference | Sustained utilization is measured and serving expertise exists | Idle accelerators, upgrades, capacity reserve, pager load | Bursty unknown demand or no serving owner |

An open-source model gateway demonstrates that the gateway pattern can be self-hosted, but its existence does not prove that an image workload needs one. Treat the repository in Further reading as implementation evidence for the pattern, not a recommendation. The other supplied reference concerns reranking rather than image generation; it is useful only as an example of why capability-specific documentation must be checked before treating “AI API” interfaces as interchangeable.

For an MVP, the least complex option that meets the acceptance and completion objectives should survive the first decision round. Stick with a direct adapter when one backend is intentional and the gateway has no policy job to do. Consider a gateway after a second backend or shared control requirement exists. Consider self-hosting only after measured utilization and staffing make the operational obligation explicit. Set review triggers now: prompt-set change, output-mode change, sustained SLO miss, acceptance shift, or a material contract change.

Keep it reversible.

## References

- https://github.com/BerriAI/litellm
- https://docs.cohere.com/docs/rerank-overview

## Further reading

- https://github.com/BerriAI/litellm
- https://docs.cohere.com/docs/rerank-overview
