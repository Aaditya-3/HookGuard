# HookGuard — Requirements

## 1. Functional Requirements

### FR-01: Integration Management

The system must allow users to create and manage webhook integrations.

When creating an integration, the user must provide:
- An integration name.
- The source service from which they want to receive updates.
- The event types they want to receive.
- The destination URL where updates should be delivered.

HookGuard must generate a unique ingress URL for each integration and associate it with the configured source service and destination.

For the initial version, users will configure the source service to send webhooks to the generated ingress URL manually. Automatic webhook registration through provider APIs will be considered in a future version.

Users should be able to inspect their integration configurations and enable or disable integrations.

### FR-02: Event Ingestion

HookGuard must expose an ingress endpoint that accepts webhook events from configured source services.

Each ingress URL must uniquely identify its associated integration so that events are routed to the correct destination.

When an event is received, HookGuard must:
1. Identify the integration using the ingress URL.
2. Validate the incoming request.
3. Determine the event type.
4. Verify that the integration is active.
5. Determine whether the event type is subscribed to.
6. Process the event according to the integration's configuration.

Invalid requests must not be forwarded to the destination.

The system must acknowledge requests according to a documented response policy.

### FR-03: Event Filtering

Users must be able to specify the event types they want to receive from a source service.

HookGuard must forward only events that match the integration's configured subscriptions.

If an event type is not subscribed to, HookGuard must not forward it to the destination. It should return an appropriate response to the source and record the filtering outcome when necessary for operational visibility.

The system must distinguish between an intentionally ignored event and an event that failed during delivery.

### FR-04: Event Forwarding

HookGuard must forward accepted events to the configured destination URL using HTTP requests.

Before forwarding an event, HookGuard must wrap its payload in a standardized event envelope containing:
- A unique and stable event ID.
- The event type.
- The event creation timestamp.
- Source or integration metadata.
- The original event payload.

The event ID must remain stable across delivery attempts for the same logical event.

HookGuard must sign outgoing webhook requests so that receiving applications can verify their authenticity and integrity.

The system must capture the HTTP response status and relevant delivery information.

### FR-05: Delivery Tracking

HookGuard must maintain a record of each logical delivery and its delivery attempts.

For each delivery, the system should record:
- The event ID.
- The associated integration.
- The destination.
- The current delivery status.
- The number of attempts.
- The time each attempt started.
- The HTTP response status, when available.
- Relevant error information.
- The time of the most recent attempt.

Delivery statuses must follow explicitly defined state transitions.

Users must be able to inspect delivery history and identify whether an event is pending, in progress, successfully delivered, scheduled for retry, or in a terminal failure state.

The system must distinguish between a successful HTTP response and a confirmed business operation at the receiving application. HookGuard cannot independently guarantee that an external application performed its intended business operation exactly once.

### FR-06: Failure Recovery

HookGuard must support delivery recovery when a destination is temporarily unavailable or a delivery attempt fails.

The system must:
- Retry eligible failed deliveries according to a configurable policy.
- Use exponential backoff with jitter to avoid excessive retry traffic.
- Apply a maximum retry limit.
- Record every delivery attempt.
- Move deliveries that exhaust their retry policy into a dead-letter or terminal-failure state.
- Support manual replay of eligible failed deliveries.
- Recover pending delivery work after a worker restarts.
- Prevent duplicate message processing from corrupting delivery state.

The system must account for the possibility that a destination processes a webhook but the response is lost. Consequently, duplicate HTTP requests may occur, and receiving applications must be able to use stable event IDs to implement idempotent processing.

HookGuard can recover an event only after it has received and durably recorded it. Recovery of events that never reach HookGuard depends on the source service's retry or replay capabilities.

### FR-07: Asynchronous Processing

HookGuard must separate event ingestion from delivery processing.

The system must accept valid events without requiring the incoming HTTP request to remain open until all delivery attempts have finished.

The architecture must support a durable message queue and independent background workers for processing delivery jobs.

The messaging design must account for worker failures, duplicate message delivery, message acknowledgements, and recovery of pending jobs.

The transactional outbox pattern must be used to coordinate database changes with the publication of delivery jobs.

### FR-08: Idempotency

HookGuard must support idempotent event submission where an appropriate idempotency key is provided.

Repeated equivalent submissions using the same key within the applicable scope must not create multiple logical events.

Requests that reuse an idempotency key with conflicting content must follow a documented error policy.

Message redelivery must not create a second logical delivery record or cause invalid delivery-state transitions.

Idempotency of event creation and idempotency of external webhook processing must be treated as separate concerns.

### FR-09: Rate Limiting and Resource Management

HookGuard must support configurable rate limits to prevent excessive requests from exhausting application resources.

Rate limits must be enforceable across multiple API instances rather than being limited to the memory of an individual process.

The system must support bounded worker concurrency and avoid allowing one continuously failing destination to monopolize all available processing capacity.

### FR-10: Correctness Testing

HookGuard must include a JavaScript-based, Jepsen-inspired correctness testing tool.

The tool must be able to:
- Generate concurrent requests and delivery operations.
- Inject controlled worker and network failures.
- Record operation histories and relevant outcomes.
- Check invariants related to idempotency, event durability, and valid state transitions.
- Detect deliveries that remain incomplete under defined recovery conditions.
- Produce reproducible reports when a correctness violation is discovered.

The checker must evaluate HookGuard against explicitly documented guarantees rather than assume that every duplicate request or failed HTTP operation represents a correctness violation.

## 2. Nonfunctional Requirements

### NFR-01: Reliability

Once HookGuard acknowledges an event as durably accepted, the event must remain recoverable after an API process restart.

Accepted work must not be silently lost because the message broker or a worker becomes temporarily unavailable.

The system must be designed to tolerate duplicate message delivery and recover eligible jobs following worker failures.

Delivery processing must follow documented state transitions, and failed attempts must remain inspectable.

The system will initially target at-least-once webhook delivery, subject to the configured retry policy and the availability of required components.

Exactly-once external business effects are not guaranteed.

### NFR-02: Security

HookGuard must protect webhook payloads and integration configuration.

Security requirements include:
- HTTPS for production ingress and delivery traffic.
- Validation of incoming requests.
- Verification of source signatures where supported, or another documented authentication mechanism.
- Cryptographic signing of outgoing webhook requests.
- Secure generation and storage of ingress credentials and signing secrets.
- Exclusion of secrets and sensitive payload data from inappropriate logs.
- Destination URL validation and protection against server-side request forgery.
- Request-size limits and appropriate timeouts.
- Protection against unauthorized integration management.

An unguessable ingress URL must not automatically be treated as sufficient proof that an event originated from a trusted source.

### NFR-03: Scalability and Performance

The system must support asynchronous processing and allow the number of background workers to be increased independently of the API instances.

PostgreSQL must remain the authoritative store for integration configuration, delivery state, and delivery history. Redis may be used for shared rate limiting, caching, and other suitable short-lived state.

RabbitMQ must distribute delivery jobs among background workers.

The initial performance target is to process at least 1,000 events per minute under a documented and reproducible load-test configuration.

This is a target to validate through k6 testing, not an existing performance guarantee.

Performance testing should measure throughput, latency, queue depth, failed-delivery rates, and recovery behavior under load.

### NFR-04: Observability

HookGuard must provide sufficient operational information to diagnose delivery failures and identify processing bottlenecks.

The system should support:
- Structured application logs.
- Delivery attempt histories.
- Metrics for incoming events, delivery throughput, retries, and terminal failures.
- Queue-depth and worker-activity monitoring.
- Request latency and error-rate measurements.
- Tracing across the API, message-processing pipeline, and delivery workers.
- Reproducible failure reports from the correctness checker.

Operational records must not expose signing secrets or other sensitive credentials.

### NFR-05: Maintainability and Testability

The codebase must use modular JavaScript components with clearly defined responsibilities.

The implementation must include unit tests, API integration tests, database-related tests, and failure-recovery tests.

Important correctness guarantees must have corresponding automated tests.

The project must provide documented setup instructions, database migrations, reproducible test commands, and a Docker Compose development environment.

Major architecture decisions should be documented so that their rationale and trade-offs can be understood during code reviews and interviews.

## 3. Initial Scope

The initial working version will focus on a single end-to-end integration workflow.

It must support:

1. Creating an integration with a source-service identifier, subscribed event types, and a destination URL.
2. Generating a unique ingress URL for the integration.
3. Receiving sample webhook events.
4. Identifying the integration associated with an incoming event.
5. Filtering events according to the configured subscriptions.
6. Wrapping accepted payloads in a standardized event envelope.
7. Forwarding events to the destination.
8. Recording basic delivery outcomes.
9. Validating inputs and testing the basic workflow.

The first version will use manually configured source webhooks and a local test receiver.

The architecture will then be extended to support the complete reliability pipeline, including PostgreSQL persistence, the transactional outbox, RabbitMQ workers, Redis rate limiting, retries, dead-letter handling, and crash recovery.

The correctness checker, network fault injection, and load testing will be added incrementally after the essential delivery workflow is operational.

## 4. Out of Scope

The following features will not be implemented in the initial version:

- Automatic webhook registration for every supported provider.
- OAuth integrations and provider-specific account management.
- Polling source APIs to discover missed events.
- Multiple destination URLs per integration.
- Advanced payload transformation and configurable routing rules.
- A distributed multi-region deployment.
- Kubernetes-based orchestration.
- A billing and subscription-management system.
- A guarantee that arbitrary external applications execute each event exactly once.
- A general-purpose replacement for the complete Jepsen testing framework.

These features may be considered after the core delivery pipeline, recovery mechanisms, and correctness checker are implemented and tested.