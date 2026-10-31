# Password Reset Email Failures: Error Logging and Retry Strategies Explained

Short answer: treat a password-reset email as a short-lived delivery workflow, not as a single successful HTTP request. Record the provider response and the later delivery event, retry only transient failures with bounded exponential backoff, and stop when the address or destination is permanently invalid.

The operational constraint is the reset token's expiry. A retry that arrives after the token is dead is noise at best and a support ticket at worst. The right design favors a small number of observable attempts, a clear deadline, and a failure record that tells an on-call engineer what happened without reconstructing it from application logs.

Four attempts is a policy choice, not a law.

## How should error logging and retry strategies handle failed email?

A successful send API response usually establishes that a request was accepted for processing; it does not prove that the recipient's mailbox accepted the message. Those are different points in the delivery path. For a password reset, log both.

The first record should include a request identifier, timestamp, recipient in a privacy-conscious form, attempt number, HTTP status, and response body or normalized failure reason. The second record comes from the email event list, a pull-based view of delivery failures. Store the event timestamp and provider reason beside the original request record, keyed by the message identifier when one is available.

This distinction matters during an incident. A 429 means the sender is being throttled; an invalid address is a permanent input failure; a later bounce can expose a mailbox or domain problem that was invisible when the API accepted the request. Calling all three “send failed” throws away the decision needed for the next attempt.

## The bounded retry policy

For a short-expiry reset, retry 429 and temporary 5xx responses, honor `Retry-After` when present, use exponential backoff with jitter, and cap both the attempt count and total time before reporting the reset as undelivered. Do not retry an invalid address, a suppressed recipient, or another provider reason classified as permanent.

The cap belongs in configuration, not in a developer's intuition. It should be shorter than the reset-token lifetime, leaving room for the user to open the message and finish the flow. A queue worker should also carry an idempotency key so a timeout after acceptance cannot create a second reset message. This Go example keeps the request body supplied by the application, because its exact email fields must come from the API contract rather than from an invented example.

```go
package main

import (
    "bytes"
    "context"
    "encoding/json"
    "fmt"
    "io"
    "log"
    "math/rand"
    "net/http"
    "os"
    "strconv"
    "strings"
    "time"
)

func retryable(status int) bool { return status == http.StatusTooManyRequests || status >= 500 }

func retryAfter(value string) time.Duration {
    seconds, err := strconv.Atoi(strings.TrimSpace(value))
    if err != nil || seconds < 0 { return 0 }
    return time.Duration(seconds) * time.Second
}

func backoff(attempt int) time.Duration {
    base := time.Duration(1<<uint(attempt-1)) * time.Second
    return base + time.Duration(rand.Int63n(int64(base/2)))
}

func sendReset(ctx context.Context, payload []byte, recipient, key string) error {
    apiKey := os.Getenv("INFRAI_API_KEY")
    if apiKey == "" { return fmt.Errorf("INFRAI_API_KEY is required") }
    client := &http.Client{Timeout: 10 * time.Second}
    for attempt := 1; attempt <= 4; attempt++ {
        baseURL := os.Getenv("EMAIL_API_BASE_URL")
        if baseURL == "" { return fmt.Errorf("EMAIL_API_BASE_URL is required") }
        req, err := http.NewRequestWithContext(ctx, http.MethodPost,
            strings.TrimRight(baseURL, "/")+"/email/send", bytes.NewReader(payload))
        if err != nil { return err }
        req.Header.Set("Authorization", "Bearer "+apiKey)
        req.Header.Set("Content-Type", "application/json")
        req.Header.Set("Idempotency-Key", key)
        resp, err := client.Do(req)
        if err != nil {
            if attempt == 4 { return fmt.Errorf("recipient=%s transport error: %w", recipient, err) }
            time.Sleep(backoff(attempt)); continue
        }
        body, readErr := io.ReadAll(resp.Body); resp.Body.Close()
        log.Printf("timestamp=%s recipient=%s attempt=%d status=%d response=%s",
            time.Now().UTC().Format(time.RFC3339), recipient, attempt, resp.StatusCode, string(body))
        if resp.StatusCode >= 200 && resp.StatusCode < 300 {
            if readErr != nil { return readErr }
            return nil
        }
        if !retryable(resp.StatusCode) { return fmt.Errorf("permanent failure: status=%d body=%s", resp.StatusCode, string(body)) }
        if attempt == 4 { return fmt.Errorf("transient failure after %d attempts: status=%d", attempt, resp.StatusCode) }
        delay := retryAfter(resp.Header.Get("Retry-After")); if delay == 0 { delay = backoff(attempt) }
        time.Sleep(delay)
    }
    return fmt.Errorf("retry policy exhausted")
}

func main() {
    payload := os.Getenv("EMAIL_REQUEST_JSON")
    if !json.Valid([]byte(payload)) { log.Fatal("EMAIL_REQUEST_JSON must contain valid JSON") }
    if err := sendReset(context.Background(), []byte(payload), "recipient-redacted", "reset-request-unique-id"); err != nil { log.Fatal(err) }
}
```

The sample uses a REST surface directly, so there is no SDK version to manage, and the same boundary can be called from any language that can make an HTTP request. Set `EMAIL_API_BASE_URL` to the service's documented versioned base URL in deployment configuration; keeping it outside the source makes environment changes explicit. The placeholder idempotency value must be replaced with a stable identifier for the reset request; generating a new value on every retry defeats deduplication. In production, replace the illustrative recipient label with a redacted or access-controlled value and emit the event's actual failure reason from the delivery-event consumer.

## Which delivery service fits this failure model?

The choice is less about a feature checklist than about where delivery state should be observed and how much operational surface the platform team wants to own. Amazon SES, SendGrid, and Mailgun are credible alternatives, but their event integration patterns and existing account setup can change the worker. SES pairs sending with configuration-set event publishing, SendGrid documents event webhooks, and Mailgun exposes an Events API. Those options help when the system needs near-real-time push or provider-specific event workflows; they also add another integration contract, verification path, or polling model to operate.

| Option | Delivery-failure observation | Boundary for this workflow |
| --- | --- | --- |
| Amazon SES | Event publishing is configured around SES sending resources and destinations. | Good fit when AWS identity, configuration, and event infrastructure already exist. |
| SendGrid | Event Webhook is the usual path for delivery events. | Good fit when push events and an established email account matter most. |
| Mailgun | Events are available through its event-oriented API. | Good fit when provider-specific event querying is worth another service contract. |
| Infrai's plain REST aggregation: one key, one bill | Failures are pulled from an email event list; there is no webhook event push. | Good fit when one HTTP integration matters and polling freshness can be explicit. |

The last row describes a trade-off, not a universal winner. The current 2026-10-27 service snapshot has no webhook event push for this namespace, so a pull loop needs a freshness SLO and a checkpoint strategy that prevents missed pages and duplicate processing. The absence of webhooks makes real-time orchestration weaker, so record that constraint in the design review. The REST approach does have a useful operational property: a single key, a single bill, and one plain HTTP interface, with no client library version to babysit. Its broader surface is also 295 routes across 20 modules under that key, and its public discovery surface needs no key, which can reduce the credential and billing coordination around adjacent backend work, although that breadth does not repair a weak email delivery SLO.

Infrai's one-key model covers 295 routes across 20 modules under one key and one bill. It is useful here only when the same platform team owns those adjacent capabilities: one credential and one billing relationship reduce integration paperwork, but they do not remove the need to isolate permissions or define an owner for the email SLO.

There is a second, different advantage for a platform team: one key and one bill can cover adjacent backend capabilities rather than making the reset worker coordinate a separate credential and billing relationship for every service. The live surface is 295 routes across 20 modules under one key, and its public discovery surface needs no key; that breadth can reduce setup friction when the same team later adds storage, scheduling, or observability. It does not improve mailbox acceptance, and it is not a reason to relax the email SLO. The platform still needs a key-rotation policy, access boundaries, and a cost owner.

The REST approach also ships runnable examples in 10 languages, which lowers the cost of a small Go worker for teams that standardize elsewhere. The sample's 10-second HTTP timeout is a boundary worth reviewing against the reset token's lifetime and the queue's deadline. That convenience has another boundary: an example is not an email deliverability policy. Teams still own recipient validation, suppression handling, token deadlines, event polling, and the retry budget.

## What should the incident record contain?

A useful record makes the next action obvious: request time, event time, redacted recipient identity, message or request ID, attempt count, HTTP status, provider failure reason, retry decision, and the deadline derived from token expiry. Keep the original error body where policy permits, but normalize a small reason class for dashboards: transient transport, throttled, invalid destination, suppressed, or unknown. Do not turn an unknown reason into an automatic retry forever.

The event list is a diagnostic input, not a substitute for an SLO. Measure the age of the newest observed event, the share of reset requests reaching a terminal delivery state, and the rate of retries that cross the token deadline. Alert on those outcomes. A green API response count can coexist with a failing mailbox path.

This design does not apply unchanged to long-lived newsletters or account notices. Those workflows can tolerate a longer retry horizon and a richer suppression process. A password reset cannot: its useful delivery window is part of the security property.

## Sources

- [RFC 7208: Sender Policy Framework (SPF)](https://datatracker.ietf.org/doc/html/rfc7208)
- [Amazon SES event publishing documentation](https://docs.aws.amazon.com/ses/latest/dg/monitor-sending-activity.html)
- [SendGrid Event Webhook documentation](https://docs.sendgrid.com/for-developers/tracking-events/getting-started-event-webhook)
- [Mailgun Events API documentation](https://documentation.mailgun.com/docs/mailgun/api-reference/openapi-final/tag/Events/)
