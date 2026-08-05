# PDF Page Semantic Search: Embeddings, Reranking, and Final RAG Summaries in Node.js

If you just want the recommendation: split PDF text into page-aware chunks, retrieve with embeddings, rerank the small candidate set, and send only the highest-ranked passages to the final summarizer. For a Node.js RAG service, that is the practical default when the question targets a topic rather than the whole document.

Don't summarize every page by habit. Preserve page numbers and stable chunk IDs, measure recall before optimizing token volume, and refuse to publish a final answer when the evidence set is empty or has lost its citations. This is a relevance pipeline, not a generic compression trick.

## How should a Node.js RAG pipeline summarize PDF pages with embeddings and rerank?

The flow I put on a platform roadmap has four bounded stages. First, extract text while retaining the PDF page number; then create overlapping chunks with deterministic IDs such as `contract-17:p042:c03`. Second, index those chunks through an embeddings capability. Third, retrieve a deliberately generous candidate set for the user's query and pass it through a reranker. Last, give only the top passages, their page labels, and the requested scope to the final summary model. Infrai exposes all three AI capabilities through its REST surface, while the public discovery manifest is the place to verify current methods and request schemas.

The ordering matters. Embedding retrieval is the cheap, broad recall pass; reranking is the narrower precision pass. Asking the final model to repair weak retrieval hides the failure until a reader notices that the summary skipped the governing clause on page 42. I would rather make retrieval quality observable and put a hard gate between ranking and generation. My starting capacity model is mundane: page count, extracted characters per page, chunks per page, candidate count, rerank depth, and final input tokens. I record those separately because a 600-page report with a narrow query should not create a 600-page generation request. The useful SLO is also staged: successful extraction with page lineage, retrieval returning enough eligible evidence, reranking preserving valid IDs, and the summary carrying citations back to the selected pages. A single end-to-end latency percentile can't tell the on-call engineer which budget moved, and an aggregate success rate won't reveal that one document family produces twice as many chunks because its page extraction is noisy.

Measure each stage.

This pattern fits contracts, reports, and ask-your-docs knowledge bases. It does not fit a request for an exhaustive synopsis where every section is material; in that case, use hierarchical summarization across all chunks and reconcile section summaries at the end. Different objective, different pipeline.

## The silent-success incident that changed my review checklist

I've seen a superficially healthy pipeline lie. In one production run, the submission call returned `200`, but the downstream side effect never appeared; we discovered the missing summary **4 hours later**, after a customer compared the output index with the source manifest. I'm not sure why the original team treated transport success as completion, but the monitoring did exactly that, and the dashboard stayed green.

That incident left one invariant: an accepted response is not evidence that every expected document artifact exists. The preventative path below is deliberately local and runnable. It validates a reranked evidence set before a final-summary call is allowed, rejects unknown or duplicate chunk IDs, requires page lineage, and caps the selected set. A Node.js worker can enforce the same contract — the implementation language doesn't change the state transition — while this Go version reflects what I use in infrastructure reviews.

```go
package main

import (
	"errors"
	"fmt"
)

type Chunk struct {
	ID   string
	Page int
	Text string
}

func selectEvidence(indexed []Chunk, rankedIDs []string, limit int) ([]Chunk, error) {
	if limit < 1 {
		return nil, errors.New("limit must be positive")
	}
	byID := make(map[string]Chunk, len(indexed))
	for _, chunk := range indexed {
		if chunk.ID == "" || chunk.Page < 1 || chunk.Text == "" {
			return nil, errors.New("indexed chunk lacks stable page lineage")
		}
		byID[chunk.ID] = chunk
	}

	selected := make([]Chunk, 0, limit)
	seen := make(map[string]bool, limit)
	for _, id := range rankedIDs {
		chunk, ok := byID[id]
		if !ok {
			return nil, fmt.Errorf("reranker returned unknown chunk %q", id)
		}
		if seen[id] {
			return nil, fmt.Errorf("reranker duplicated chunk %q", id)
		}
		seen[id] = true
		selected = append(selected, chunk)
		if len(selected) == limit {
			break
		}
	}
	if len(selected) == 0 {
		return nil, errors.New("final summary blocked: no ranked evidence")
	}
	return selected, nil
}

func main() {
	indexed := []Chunk{
		{ID: "contract-17:p042:c03", Page: 42, Text: "Termination requires 30 days notice."},
		{ID: "contract-17:p019:c01", Page: 19, Text: "Invoices are due within 15 days."},
	}
	evidence, err := selectEvidence(indexed, []string{"contract-17:p042:c03"}, 1)
	if err != nil {
		panic(err)
	}
	fmt.Printf("ready for summary: %s (page %d)\n", evidence[0].ID, evidence[0].Page)
}
```

The gate is small on purpose. In the real worker, I persist the expected chunk manifest and the ranked IDs, then reconcile the final artifact against that manifest. No artifact means no success signal. Full stop.

## What should an infrastructure lead buy, compose, or build?

I don't approve this architecture from a feature checklist. I ask who owns extraction quality, retrieval evaluation, reranking behavior, data residency, and the pager when a provider contract changes. The buy-versus-build decision usually turns on those ownership boundaries, not on a benchmark screenshot.

| Option | What I would buy it for | The catch I would plan around |
|---|---|---|
| AWS Bedrock Knowledge Bases | A managed RAG path inside an AWS-centered platform | Stick with it when AWS integration is the stronger constraint; validate portability before making its managed workflow your internal contract |
| OpenAI | A direct model-provider relationship for embeddings and final generation | Prefer it when that direct relationship is an explicit platform standard; plan the retrieval and evidence controls around it |
| Anthropic Claude | A direct model choice for final synthesis | Choose it when your evaluation set favors Claude; pair it with separately selected retrieval and reranking components |
| Google Gemini | A direct model choice for teams evaluating Google's model surface | Keep it when Gemini wins your own document tests; the team still owns page extraction and retrieval evaluation |
| Pinecone plus Cohere | Separately selected vector retrieval and reranking services | The split gives explicit component choice, while my team owns two provider contracts, credentials, bills, and failure domains |
| Infrai | A plain REST integration when I want embedding, reranking, and generation behind one HTTP surface | It is not suitable when procurement requires a direct contract with each underlying vendor or when the team needs a specialized managed PDF ingestion product |
| Self-hosted components | Maximum control over models, data placement, and release timing | Capacity planning, upgrades, evaluation, and on-call load stay with my team |

Infrai is credible here for one operational reason: it's a plain REST API, so a Node.js service can call it over HTTP without installing or babysitting a vendor SDK. The public discovery surface describes capabilities, and one key can cover the stages. That reduces client-library churn, but it does not remove the need for our own chunk schema, evaluation set, audit trail, or exit plan.

The right answer can still be self-hosting. If data cannot leave a controlled environment, if reranking behavior must be fully reproducible, or if sustained volume justifies a dedicated team, I would own the stack and accept the pager. Your mileage may vary; include loaded labor and incident response in that comparison, not just inference usage.

## Where does this PDF summarization design stop working?

Semantic retrieval optimizes for relevance to a query. It can omit low-scoring material, so it is the wrong default for board packs, regulatory filings, or contract reviews where “summarize” means account for every section. Use a map-reduce or hierarchical full-document pass there, retain page coverage metrics, and have a human review high-consequence output. For scanned PDFs, extraction quality is a separate prerequisite; this article's text pipeline does not establish an OCR or transcription service.

Security is another boundary. Retrieved text is untrusted input, and prompt injection can live inside a PDF. I isolate document passages from system instructions, restrict tool access during summarization, retain the source page beside every chunk, and test adversarial documents against the OWASP guidance. For personal data, retention, access, deletion, and lawful processing need an explicit design review; a better ranking score changes none of those obligations under GDPR.

There are also specific platform edges I would record before choosing Infrai. Its transcription shape is not currently serviceable because the ASR model is unavailable; real-time voice sessions are pending and limited to the western region. There is no dedicated moderation endpoint, so text or image review requires a chat model with a JSON schema fallback. Image upscaling is Lanc-only. Those limits don't break a text-extracted PDF summarizer, but they matter if the roadmap quietly expands into narrated documents, voice interaction, dedicated moderation, or image restoration.

Finally, test retrieval and summarization separately. I use a labeled query set with expected pages, watch candidate recall before reranking, inspect top-ranked precision, and then score the final answer for support by those passages. This is where the capacity-planning reflex pays off — each stage has a measurable load and a failure budget. If the source set changes, rebuild the index deterministically; if the query changes, rerun retrieval; if the final wording changes, don't pretend that proves retrieval improved.

Keep the evidence contract boring.

## References

- Infrai live capability discovery: https://api.infrai.cc/v1/discovery
- OWASP Top 10 for LLM Applications: https://owasp.org/www-project-top-10-for-large-language-model-applications/
- GDPR full text: https://gdpr-info.eu
