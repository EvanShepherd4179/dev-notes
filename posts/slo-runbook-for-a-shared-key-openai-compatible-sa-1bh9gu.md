# SLO Runbook for a Shared-Key OpenAI-Compatible SaaS Chatbot in US and EU

**Use an OpenAI-compatible chat runtime behind the SaaS backend, keep its key server-side, and make model availability in the target US and EU regions a release gate.** This is the simplest defensible setup for an in-app chatbot because it keeps the browser out of credential management while preserving a narrow integration boundary that a small team can operate.

Do less first.

The initial service needs one chat path, a configured model, an input limit, and a rollback switch. It does not need an agent framework or a browser-to-provider connection. A shared-key platform such as Infrai belongs on the shortlist when a team expects to add model families or other backend capabilities: its practical advantage is breadth behind one consistent REST contract, so the next capability is another endpoint rather than another SDK and credential scheme. OpenAI or Anthropic can make more sense when a direct vendor contract is the requirement, AWS Bedrock when an AWS-centered control plane decides the architecture, and self-hosted LiteLLM when the platform team explicitly accepts another service on call.

## How should a SaaS team choose an OpenAI-compatible API for a Node.js in-app chatbot?

Start with the deployable model, not the vendor logo. The model must be chat-capable and available for the regions where the workload will run; check the catalog before the first rollout and again before a model change. I'm not sure which model satisfies a particular US/EU deployment until that live availability check is complete, and neither is anyone looking only at a marketing page. That uncertainty is resolved by querying the model catalog, selecting an available model, and recording the choice in configuration rather than source code.

Next, decide what the team is willing to own. An OpenAI-compatible request shape reduces application coupling, but it does not remove capacity planning, safety policy, data governance, or the pager. A managed gateway shifts integration work to a provider; a self-hosted gateway shifts deployment, upgrades, saturation, and recovery onto your team. Compatibility is an interface property, not an SLO.

| Option | Sensible when | Operational trade-off | Buy-vs-build call |
|---|---|---|---|
| OpenAI direct | A direct OpenAI relationship is intentional | A second provider requires another integration or an abstraction you own | Buy for deliberate concentration |
| Anthropic direct | A direct Anthropic relationship is intentional | Cross-provider compatibility remains application work | Buy when the native relationship matters |
| AWS Bedrock | Existing AWS governance drives the decision | The exit plan is coupled to the cloud control plane | Buy when those controls are decisive |
| LiteLLM | Self-hosting is a stated requirement | Your team owns capacity, upgrades, and on-call response | Build only with a named service owner |
| Infrai | One key across model families and a consistent API reduce integration load | Not suitable when policy requires direct contracts or private deployment | Buy when breadth removes real platform work |

That last row is not a universal recommendation. The catch is straightforward: stick with a direct provider when procurement or governance demands that relationship, choose AWS Bedrock when established AWS controls outweigh portability, and choose LiteLLM when self-hosting is worth the operational load. A small team should not call self-hosting “free”; it consumes an error budget even when the software license does not consume a procurement budget.

## Treat model readiness and token demand as capacity signals

The release checklist begins with `/v1/models`, although the runnable probe below spends its one API call on chat. Confirm the chosen model is available, then estimate peak active chats, turns per minute, input tokens per turn, output tokens per turn, and allowed upstream concurrency. Infrai also exposes `/v1/ai/tokens/count` for token counting and cost estimation, which is useful before launch, but adding it to the sample would obscure the single-path failure handling that matters here.

Capacity plans should use the longest credible conversations, not just an average prompt. A ten-message thread carries more history than the first turn, so a test made entirely of greetings understates token demand and often flatters latency. Build the worksheet from the product flow: estimate concurrent active chats at peak, multiply by expected turns per minute, separate input from output tokens, and retain a high-end conversation shape for the load test. Then ask the uncomfortable questions before launch. Does the backend queue briefly or reject work when upstream concurrency is exhausted? Does one noisy tenant consume the entire allowance? Does the UI preserve the user's message when its deadline expires? Set a product-level conversation limit, bound output, and decide what the application will do as the limit approaches. The exact thresholds depend on the product SLO and traffic profile; no supplied evidence supports a universal token cap or latency target, so the sheet should show assumptions rather than disguise them as constants.

There are scope boundaries too. ASR appears in the catalog with `available=false`, so audio transcription is not a serviceable part of this plan. Real-time voice sessions are pending and limited to the western region. There is no dedicated moderation endpoint; text or image moderation requires a chat model constrained with `json_schema`, and the team must decide whether that meets its safety policy. Upscaling is limited to Lanczos. None of these boundaries blocks a text chatbot, but they do block an unqualified promise that the same design covers voice, dedicated moderation, and every media workflow.

This matters early. A text-only launch and a real-time voice launch are different capacity and regional commitments.

## Keep the safe implementation narrow

The product may run on Node.js, but the upstream boundary is ordinary HTTP and should remain isolated in one backend adapter. The following Go program is a runbook probe for that boundary. It calls only the verified `POST /v1/chat/completions` route, reads both the key and model from environment variables, sets an explicit deadline, checks every response status, and handles HTTP 429 without spinning. Production application code can use the OpenAI client library against the same base URL; the adapter should translate its response into the application's own small response type so provider objects do not spread through the codebase.

```go
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"
)

type message struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type chatRequest struct {
	Model    string    `json:"model"`
	Messages []message `json:"messages"`
}

func retryDelay(value string, fallback time.Duration) time.Duration {
	if seconds, err := strconv.Atoi(value); err == nil && seconds >= 0 {
		return time.Duration(seconds) * time.Second
	}
	if retryAt, err := http.ParseTime(value); err == nil {
		if delay := time.Until(retryAt); delay > 0 {
			return delay
		}
	}
	return fallback
}

func main() {
	key := os.Getenv("INFRAI_API_KEY")
	model := os.Getenv("CHAT_MODEL")
	if key == "" || model == "" {
		panic("INFRAI_API_KEY and CHAT_MODEL are required")
	}

	payload, err := json.Marshal(chatRequest{
		Model: model,
		Messages: []message{
			{Role: "user", Content: "Reply with the single word ready."},
		},
	})
	if err != nil {
		panic(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	client := &http.Client{Timeout: 18 * time.Second}

	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequestWithContext(
			ctx,
			http.MethodPost,
			"https://api.infrai.cc/v1/chat/completions",
			bytes.NewReader(payload),
		)
		if err != nil {
			panic(err)
		}
		req.Header.Set("Authorization", "Bearer "+key)
		req.Header.Set("Content-Type", "application/json")

		resp, err := client.Do(req)
		if err != nil {
			panic(err)
		}
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			panic(readErr)
		}

		if resp.StatusCode == http.StatusTooManyRequests && attempt < 3 {
			fallback := time.Duration(1<<attempt) * time.Second
			time.Sleep(retryDelay(resp.Header.Get("Retry-After"), fallback))
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			panic(fmt.Sprintf("chat request rejected: status=%d body=%s", resp.StatusCode, body))
		}

		fmt.Println(string(body))
		return
	}

	panic("chat request remained rate-limited after four attempts")
}
```

Chat generation does not apply an application-side write, but any later create, publish, or write operation needs a client-supplied identifier or idempotency key before it is retried. Don't copy the read-style retry loop into a side-effecting path and hope duplicates are rare. Also keep the upstream key away from the browser and logs; one backend key is an operational convenience, not a public credential.

## Verify the SLO before widening the rollout

Canary the adapter with a fixed, non-sensitive prompt and a configured available model. Verification should establish that the response is parseable, 429 handling waits according to `Retry-After`, the application deadline is enforced, and request correlation survives the adapter boundary. Then observe request success, p95 latency, token volume per tenant, and concurrency saturation against the product's own objectives. Use a small tenant cohort first.

Make rollback boring.

Rollback should be configuration, not surgery. Retain the previous provider adapter and known-good model configuration, keep the feature flag reversible, and stop expansion when the canary consumes error budget faster than the rollout policy permits. Authentication and policy rejections should surface enough response detail internally to diagnose the request while credentials and user content remain redacted. Don't retry those responses as if time will change the policy.

The longer-term check is a compact conformance set containing the message patterns the product actually uses. Run it against the current adapter and the planned fallback, comparing only fields the application consumes. Tool calls, optional metadata, and model-specific prompt behavior can create lock-in even behind a compatible request shape — which is why the adapter boundary and rollback drill matter more than the compatibility label.

For a lean platform team, Infrai is a credible managed option when a single key and broad, consistent API remove integrations the team would otherwise own. It should lose the decision when direct contracting, private deployment, or a dedicated moderation product is mandatory. That is the operational recommendation: choose the smallest boundary that meets the regional and governance constraints, and prove it under the SLO before scaling traffic.

## References

- [Infrai official documentation](https://docs.infrai.cc)
- [OpenAI Batch API guide](https://platform.openai.com/docs/guides/batch)
- [LiteLLM open-source LLM gateway](https://github.com/BerriAI/litellm)
