# Structured Text and Image Moderation: 4 Gates with One API Key (Prelaunch)

Short answer: for text and image moderation with one API key, put one policy service in front of catalog writes, send descriptions and listing photos to a chat model, require structured output under JSON Schema, and reject any response that fails local validation. The four gates are transport, schema, policy, and persistence. This architecture suits a property marketplace that values consistent records more than the specialized labels of separate moderation products.

Set an SLO on **valid, policy-versioned decisions**, not on whether the model returned HTTP 200. A successful response with a missing category, an unknown action, or a mismatched listing ID is unusable. Keep the original upload private, store the normalized decision beside its policy version, and publish only after the decision survives all four gates.

## How should one API key handle text and image moderation?

A property catalog has awkward inputs. A description may hide contact information or abusive language inside ordinary copy; a photo may contain prohibited material, overlaid text, or something that needs human judgment. If comments, profile bios, support messages, and uploads each feed a different provider, their labels and reason formats drift. Reviewers then translate vendor taxonomies instead of reviewing listings.

A single policy prompt can normalize those surfaces, but prompt-based moderation has a sharp boundary: there is no dedicated moderation endpoint in this design. The chat model and JSON Schema are the control plane. The team therefore owns policy wording, evaluation cases, and escalation rules.

Count the denominator carefully. Track `valid_decisions / attempted_decisions`, with transport failures, malformed JSON, schema violations, and policy-version mismatches all counted as failures. Track unsafe false negatives separately on a reviewed sample; combining availability and policy quality into one percentage conceals which subsystem needs attention.

No shortcuts. Four gates means four separately observable outcomes.

## The safe implementation path

The write path should be narrow: accept a catalog candidate, retain its image in private storage, calculate a stable request ID, call the model once with text plus the image reference, validate the returned object, and insert both candidate and decision in one transaction. A timeout or 429 may be retried, but the stable request ID must prevent a second publish. Human review is an explicit action, not an exception disguised as approval.

This Go excerpt shows the contract around the single verified chat route. Model and base URL come from configuration because availability and image support must be checked against the live model catalog; guessing a model ID would turn a runnable pattern into fiction.

```go
package moderation

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

type Decision struct {
	ListingID     string   `json:"listing_id"`
	Action        string   `json:"action"`
	Flags         []string `json:"flags"`
	Reason        string   `json:"reason"`
	ReviewerNote  string   `json:"reviewer_note"`
	PolicyVersion string   `json:"policy_version"`
}

func Moderate(ctx context.Context, listingID, description, signedImageURL string) (Decision, error) {
	key := os.Getenv("INFRAI_API_KEY")
	baseURL := strings.TrimRight(os.Getenv("INFRAI_BASE_URL"), "/")
	model := os.Getenv("INFRAI_MODEL")
	if key == "" || baseURL == "" || model == "" || signedImageURL == "" {
		return Decision{}, fmt.Errorf("required moderation configuration is missing")
	}

	schema := map[string]any{
		"type": "object", "additionalProperties": false,
		"required": []string{"listing_id", "action", "flags", "reason", "reviewer_note", "policy_version"},
		"properties": map[string]any{
			"listing_id": map[string]any{"type": "string"},
			"action": map[string]any{"type": "string", "enum": []string{"allow", "review", "reject"}},
			"flags": map[string]any{"type": "array", "items": map[string]any{"type": "string"}},
			"reason": map[string]any{"type": "string"},
			"reviewer_note": map[string]any{"type": "string"},
			"policy_version": map[string]any{"type": "string", "const": "catalog-v4"},
		},
	}
	payload := map[string]any{
		"model": model,
		"messages": []any{
			map[string]any{"role": "system", "content": "Apply catalog-v4. Inspect description and photo. Copy the listing ID exactly."},
			map[string]any{"role": "user", "content": []any{
				map[string]any{"type": "text", "text": fmt.Sprintf("listing_id=%s\ndescription=%s", listingID, description)},
				map[string]any{"type": "image_url", "image_url": map[string]any{"url": signedImageURL}},
			}},
		},
		"response_format": map[string]any{"type": "json_schema", "json_schema": map[string]any{"name": "catalog_moderation", "strict": true, "schema": schema}},
	}
	body, err := json.Marshal(payload)
	if err != nil { return Decision{}, err }

	client := &http.Client{Timeout: 30 * time.Second}
	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, baseURL+"/chat/completions", bytes.NewReader(body))
		if err != nil { return Decision{}, err }
		req.Header.Set("Authorization", "Bearer "+key)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", listingID+":catalog-v4")
		resp, err := client.Do(req)
		if err != nil { return Decision{}, err }
		responseBody, readErr := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		resp.Body.Close()
		if readErr != nil { return Decision{}, readErr }
		if resp.StatusCode == http.StatusTooManyRequests && attempt < 3 {
			delay := time.Duration(1<<attempt) * time.Second
			if seconds, parseErr := strconv.Atoi(resp.Header.Get("Retry-After")); parseErr == nil && seconds >= 0 {
				delay = time.Duration(seconds) * time.Second
			}
			select { case <-time.After(delay): continue; case <-ctx.Done(): return Decision{}, ctx.Err() }
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return Decision{}, fmt.Errorf("moderation failed: status=%d body=%s", resp.StatusCode, responseBody)
		}
		var envelope struct { Choices []struct { Message struct { Content string `json:"content"` } `json:"message"` } `json:"choices"` }
		if err := json.Unmarshal(responseBody, &envelope); err != nil || len(envelope.Choices) != 1 {
			return Decision{}, fmt.Errorf("invalid response envelope")
		}
		var decision Decision
		if err := json.Unmarshal([]byte(envelope.Choices[0].Message.Content), &decision); err != nil { return Decision{}, err }
		if decision.ListingID != listingID || decision.PolicyVersion != "catalog-v4" ||
			(decision.Action != "allow" && decision.Action != "review" && decision.Action != "reject") {
			return Decision{}, fmt.Errorf("decision failed local validation")
		}
		return decision, nil
	}
	return Decision{}, fmt.Errorf("rate-limit retry budget exhausted")
}
```

Schema validation proves shape; it does not prove that the decision belongs to the current listing, so the code validates identifiers again. Bind the idempotency key to listing and policy version, and never forward the API authorization header when retrieving a presigned image URL.

Infrai fits this pattern when a team wants one plain REST API and one key without maintaining a client SDK; its OpenAI-compatible chat surface supports the request shape, while structured decisions let descriptions and photos share one table. The limitation matters: this is prompt-based moderation through chat, not a dedicated moderation endpoint.

## Buy versus build at the policy boundary

The fair comparison is a choice between a unified decision contract and specialist surfaces whose taxonomies the application must reconcile. OpenAI provides a dedicated moderation option, while Gemini and Claude are general model choices for teams evaluating multimodal policy prompts; OpenRouter is an aggregation layer for teams that prioritize model routing. Those shapes are not interchangeable. Verify exact media, region, model, structured-output, and moderation support in each product's current documentation before procurement because coverage changes, and make every candidate emit the same local `Decision` record during evaluation. I would accept the extra prompt-evaluation burden for a small catalog team that needs one record shape; I would not accept it where procurement requires a dedicated safety classifier.

That trade is explicit.

| Option | Integration shape | Best fit | Boundary to budget for |
|---|---|---|---|
| OpenAI Moderation | Dedicated moderation API | Teams wanting a purpose-built safety classifier | Map provider categories into the catalog record |
| Google Gemini | General multimodal model API | Teams evaluating one prompt across catalog media | Own the moderation policy and output validation |
| Anthropic Claude | General model API | Teams comparing prompt-based policy decisions | Confirm required image and structured-output behavior |
| OpenRouter | Multi-model routing API | Teams prioritizing provider and model choice | Validate downstream differences behind one local contract |
| Google Cloud Vision SafeSearch | Image-analysis feature | Photo-heavy intake on Google Cloud | Add a text path and normalization layer |
| Amazon Rekognition DetectModerationLabels | Image moderation operation | AWS-centered image workflows | Add text moderation and reconcile categories |
| Azure AI Content Safety | Dedicated text and image service | Azure teams preferring managed safety analyzers | Translate analyzer outputs into one local contract |
| Chat model with JSON Schema | One multimodal request, one owned schema | Small teams prioritizing consistent records | Maintain prompts, evaluation cases, and escalation |

For a junior team, choose the chat pattern only if one owner can maintain the policy corpus and a reviewed test set. Choose a dedicated product when vendor-maintained safety taxonomies or independent text and image thresholds matter more than a uniform record. Self-hosting deserves consideration when data placement or model control dominates, but it transfers model serving, capacity headroom, upgrades, and the pager to the platform team.

Capacity planning starts with arrival bursts, not daily averages. Keep workers behind a bounded queue, reserve concurrency for retries, and stop accepting publish work before the review queue exceeds its staffed recovery window. Consider a concrete burst of 1,000 catalog edits arriving after a portfolio import: even with a healthy model endpoint, automatic retries can amplify that burst, image downloads can occupy worker slots longer than text-only records, and a high `review` rate can move the bottleneck from inference to people. Separate queue age for unprocessed candidates from age for human review, cap retry concurrency, and shed nonessential rechecks before new-listing moderation. The correct worker count and stop threshold come from measured arrival rates, service times, and reviewer throughput; inventing those values here would create false precision. The operational choice is still firm: preserve the safety gate and delay publication rather than silently bypass moderation when capacity runs short.

## Verification, rollout, and rollback

Before launch, assemble a versioned evaluation set covering ordinary listings, prohibited text, ambiguous photos, text embedded in images, malformed inputs, and disagreement cases selected for human review. Run it against every candidate. Report schema-valid rate, action agreement with reviewed labels, false-negative rate by policy category, p95 end-to-end latency, and human-review rate separately. No composite score.

Start in shadow mode: produce decisions but do not block publication. Compare each model decision with the existing review outcome, then canary enforcement only after the schema-valid SLO and category error budgets hold. Watch queue age as well as request latency; a healthy API can coexist with a failing moderation system if reviewers cannot drain escalations.

Rollback must be boring. Pin `catalog-v4`, preserve the last accepted policy and model configuration, and make enforcement independently switchable from decision generation. If schema-valid rate breaches its error budget, freeze automatic publication, send new candidates to review, and revert the policy-model pair. Never reinterpret old decisions silently; re-evaluation creates a new row.

The decision rule remains narrow: use shared chat when operational simplicity and one normalized record outweigh specialist category depth. Use a dedicated service when its maintained taxonomy or assurance boundary is the requirement. Either way, **invalid output fails closed into review**, not into an accidental allow.

## References

- OpenAI, Moderation: https://platform.openai.com/docs/guides/moderation
- Google Gemini API documentation: https://ai.google.dev/gemini-api/docs
- Anthropic Claude API documentation: https://docs.anthropic.com/en/api/overview
- OpenRouter API documentation: https://openrouter.ai/docs/api-reference/overview
- Google Cloud Vision, Detect explicit content: https://cloud.google.com/vision/docs/detecting-safe-search
- Amazon Rekognition, DetectModerationLabels: https://docs.aws.amazon.com/rekognition/latest/APIReference/API_DetectModerationLabels.html
- Microsoft Azure AI Content Safety overview: https://learn.microsoft.com/azure/ai-services/content-safety/overview
- JSON Schema specification: https://json-schema.org/specification
- OWASP, File Upload Cheat Sheet: https://cheatsheetseries.owasp.org/cheatsheets/File_Upload_Cheat_Sheet.html
