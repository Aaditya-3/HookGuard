HookGuard — Problem Definition
Purpose
HookGuard is a webhook relay and delivery platform. A user configures a source service, chooses the event types they care about, and provides a destination URL. HookGuard generates a unique ingress URL for the integration, receives events sent by the configured source, filters subscribed event types, wraps accepted events in a consistent envelope, and forwards them to the destination.

The long-term goal is to add durable event tracking, retries, recovery, and a JavaScript correctness checker that tests the platform under concurrency and injected failures.
Actors
- Integration owner: Creates and manages integrations, selects event types, and provides a destination URL.
- Source service: Generates events and sends webhook HTTP requests to HookGuard's ingress URL. The source must support webhooks and be configured to use this URL.
- HookGuard: Receives and validates events, filters subscribed event types, tracks delivery activity, and forwards events.
- Destination application: Receives the forwarded webhook and performs the customer's business logic.
- Correctness test harness: A development/test component that generates concurrent operations, injects controlled failures, records histories, and checks defined invariants.
Main workflow
1. The owner creates an integration with a name, source-service identifier, subscribed event types, and destination URL.
2. HookGuard validates the configuration and generates a unique ingress URL for that integration.
3. The owner configures the source service to send relevant webhook events to the ingress URL. The first version uses manual provider configuration; automatic provider setup is postponed.
4. The source sends an event to HookGuard.
5. HookGuard validates the request and identifies the integration from the ingress URL.
6. HookGuard checks whether the event type is subscribed.
7. An accepted event is wrapped in an envelope containing a stable event ID, event type, timestamp, source/integration metadata, and the original payload.
8. HookGuard forwards the envelope to the configured destination and records the outcome.
9. The owner can inspect delivery results. Later milestones add queued delivery, durable retries, dead-letter handling, replay, and correctness testing.
Initial scope
The first slice supports integration creation through an API, a unique ingress URL per integration, source-service labels, event subscriptions, one destination URL, input validation, and persistence in PostgreSQL. It includes health endpoints and automated API tests.

Deferred features include provider-specific automatic setup/OAuth, multiple destinations, advanced routing and payload transformation, RabbitMQ-based asynchronous delivery, the transactional outbox, retries, dead-letter handling, replay, Redis-backed rate limiting, distributed worker coordination, the correctness checker, fault injection, load testing, and production observability.
Security and delivery boundaries
- An ingress URL is a secret-bearing identifier, not a complete source-authentication strategy. Production ingress must verify provider signatures or another documented authentication method.
- Production delivery must use HTTPS and a receiver-verifiable signature.
- Destination URLs are untrusted. Before any outbound delivery is enabled, implement SSRF protections for loopback/private/link-local/metadata addresses, DNS rebinding, and unsafe redirects.
- HookGuard can only recover events after it receives and durably records them. If the source never successfully sends an event, recovery depends on source retries or replay.
- Delivery to an arbitrary receiver may happen more than once. HookGuard will document at-least-once behavior and stable event IDs rather than promise exactly-once external side effects.
First-slice success criteria
- A valid integration can be created and its ingress URL returned.
- The integration configuration is persisted in PostgreSQL.
- Invalid requests are rejected before database access.
- The ingress token is stored as a hash rather than as the raw token.
- Health endpoints distinguish process liveness from database readiness.
- Automated tests cover successful and invalid requests.