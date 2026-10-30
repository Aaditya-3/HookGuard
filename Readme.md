# HookGuard

### Reliable, Distributed Webhook Delivery Platform

HookGuard is a webhook delivery platform designed to reliably ingest,
persist, queue, and deliver events to external HTTP endpoints.

It separates webhook ingestion from delivery processing using
PostgreSQL, RabbitMQ, Redis, and independently running workers.

The system focuses on delivery reliability, idempotency, retries,
failure recovery, rate limiting, and observability.

---

## Architecture

```text
                  External Source
                         |
                         |
                         v
                 HookGuard API
                    Express
                         |
             +-----------+-----------+
             |                       |
             v                       v
          Redis                  PostgreSQL
       Rate Limiting             Events
                                Deliveries
                                Outbox
                                   |
                                   |
                                   v
                            Outbox Publisher
                                   |
                                   v
                               RabbitMQ
                                   |
                         +---------+---------+
                         |                   |
                         v                   v
                      Worker 1            Worker N
                         |
                         v
                  Destination Validation
                         |
                         v
                    HTTP Delivery
                         |
                         v
                  External Webhook
                    Destination