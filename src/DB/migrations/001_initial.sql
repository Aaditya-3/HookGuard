-- HookGuard initial database schema.
-- This migration creates the core entities required for webhook delivery.

-- ============================================================
-- 1. INTEGRATIONS
-- ============================================================

CREATE TABLE integrations (
    id UUID PRIMARY KEY,

    name VARCHAR(100) NOT NULL,

    source_service VARCHAR(100) NOT NULL,

    subscribed_event_types TEXT[] NOT NULL,

    destination_url TEXT NOT NULL,

    -- SHA-256 hash of the raw ingress token.
    -- The raw token is returned to the user only at creation.
    ingress_token_hash CHAR(64) NOT NULL UNIQUE,

    -- Encrypted using AES-256-GCM by the application.
    destination_signing_secret_enc TEXT NOT NULL,

    status VARCHAR(20) NOT NULL DEFAULT 'active',

    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT integrations_valid_status
        CHECK (status IN ('active', 'disabled')),

    CONSTRAINT integrations_have_event_types
        CHECK (CARDINALITY(subscribed_event_types) > 0)
);

CREATE INDEX integrations_created_at_idx
    ON integrations (created_at DESC);

CREATE INDEX integrations_status_idx
    ON integrations (status);


-- ============================================================
-- 2. EVENTS
-- ============================================================

CREATE TABLE events (
    id UUID PRIMARY KEY,

    integration_id UUID NOT NULL
        REFERENCES integrations(id)
        ON DELETE RESTRICT,

    -- The source's event ID, when the source provides one.
    source_event_id VARCHAR(256),

    event_type VARCHAR(200) NOT NULL,

    payload JSONB NOT NULL,

    received_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT events_payload_is_object
        CHECK (JSONB_TYPEOF(payload) = 'object'),

    CONSTRAINT events_nonempty_type
        CHECK (LENGTH(TRIM(event_type)) > 0)
);

-- A source event ID must not create duplicate logical events
-- within one integration. PostgreSQL permits multiple NULL
-- values in this unique constraint.
CREATE UNIQUE INDEX events_source_id_unique_idx
    ON events (integration_id, source_event_id);

CREATE INDEX events_integration_received_idx
    ON events (integration_id, received_at DESC);

CREATE INDEX events_type_received_idx
    ON events (event_type, received_at DESC);


-- ============================================================
-- 3. DELIVERIES
-- ============================================================

CREATE TABLE deliveries (
    id UUID PRIMARY KEY,

    event_id UUID NOT NULL UNIQUE
        REFERENCES events(id)
        ON DELETE RESTRICT,

    integration_id UUID NOT NULL
        REFERENCES integrations(id)
        ON DELETE RESTRICT,

    -- Snapshot the destination so later integration changes
    -- do not silently redirect an existing delivery.
    destination_url TEXT NOT NULL,

    status VARCHAR(20) NOT NULL DEFAULT 'queued',

    -- Total attempts over the delivery's lifetime.
    attempt_count INTEGER NOT NULL DEFAULT 0,

    -- Attempts in the current retry/replay cycle.
    cycle_attempt_count INTEGER NOT NULL DEFAULT 0,

    -- Number of explicit manual replay cycles.
    replay_count INTEGER NOT NULL DEFAULT 0,

    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    last_error TEXT,

    -- Used by workers to coordinate ownership and recovery.
    lease_owner VARCHAR(200),

    lease_expires_at TIMESTAMPTZ,

    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT deliveries_valid_status
        CHECK (
            status IN (
                'queued',
                'processing',
                'retry_wait',
                'succeeded',
                'dead_letter'
            )
        ),

    CONSTRAINT deliveries_nonnegative_attempt_count
        CHECK (attempt_count >= 0),

    CONSTRAINT deliveries_nonnegative_cycle_attempt_count
        CHECK (cycle_attempt_count >= 0),

    CONSTRAINT deliveries_nonnegative_replay_count
        CHECK (replay_count >= 0),

    CONSTRAINT deliveries_lease_consistency
        CHECK (
            (lease_owner IS NULL AND lease_expires_at IS NULL)
            OR
            (lease_owner IS NOT NULL AND lease_expires_at IS NOT NULL)
        )
);

CREATE INDEX deliveries_status_next_attempt_idx
    ON deliveries (status, next_attempt_at);

CREATE INDEX deliveries_integration_created_idx
    ON deliveries (integration_id, created_at DESC);

CREATE INDEX deliveries_expired_lease_idx
    ON deliveries (lease_expires_at)
    WHERE status = 'processing';


-- ============================================================
-- 4. DELIVERY ATTEMPTS
-- ============================================================

CREATE TABLE delivery_attempts (
    id UUID PRIMARY KEY,

    delivery_id UUID NOT NULL
        REFERENCES deliveries(id)
        ON DELETE RESTRICT,

    -- Lifetime attempt number for this delivery.
    attempt_number INTEGER NOT NULL,

    outcome VARCHAR(30) NOT NULL,

    http_status INTEGER,

    error_code VARCHAR(100),

    error_message TEXT,

    duration_ms INTEGER,

    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    completed_at TIMESTAMPTZ,

    CONSTRAINT delivery_attempts_positive_number
        CHECK (attempt_number > 0),

    CONSTRAINT delivery_attempts_valid_outcome
        CHECK (
            outcome IN (
                'started',
                'succeeded',
                'http_error',
                'timeout',
                'network_error',
                'internal_error'
            )
        ),

    CONSTRAINT delivery_attempts_valid_http_status
        CHECK (
            http_status IS NULL
            OR http_status BETWEEN 100 AND 599
        ),

    CONSTRAINT delivery_attempts_nonnegative_duration
        CHECK (duration_ms IS NULL OR duration_ms >= 0),

    CONSTRAINT delivery_attempts_unique_number
        UNIQUE (delivery_id, attempt_number)
);

CREATE INDEX delivery_attempts_history_idx
    ON delivery_attempts (delivery_id, attempt_number);


-- ============================================================
-- 5. TRANSACTIONAL OUTBOX
-- ============================================================

CREATE TABLE outbox_messages (
    id UUID PRIMARY KEY,

    topic VARCHAR(200) NOT NULL,

    -- Identifies the delivery or aggregate affected by the event.
    aggregate_id UUID NOT NULL,

    payload JSONB NOT NULL,

    -- Number of publication attempts, not webhook delivery attempts.
    attempts INTEGER NOT NULL DEFAULT 0,

    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    published_at TIMESTAMPTZ,

    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    -- Durable publisher claim; it can expire after a crash.
    locked_by VARCHAR(200),

    locked_until TIMESTAMPTZ,

    last_error TEXT,

    CONSTRAINT outbox_nonnegative_attempts
        CHECK (attempts >= 0),

    CONSTRAINT outbox_valid_payload
        CHECK (JSONB_TYPEOF(payload) = 'object'),

    CONSTRAINT outbox_lock_consistency
        CHECK (
            (locked_by IS NULL AND locked_until IS NULL)
            OR
            (locked_by IS NOT NULL AND locked_until IS NOT NULL)
        )
);

-- The publisher scans unpublished, eligible messages.
-- Published messages are excluded from this partial index.
CREATE INDEX outbox_pending_idx
    ON outbox_messages (next_attempt_at, created_at)
    WHERE published_at IS NULL;

CREATE INDEX outbox_aggregate_idx
    ON outbox_messages (aggregate_id, created_at);


-- ============================================================
-- 6. IDEMPOTENCY KEYS
-- ============================================================

CREATE TABLE idempotency_keys (
    id UUID PRIMARY KEY,

    integration_id UUID NOT NULL
        REFERENCES integrations(id)
        ON DELETE RESTRICT,

    idempotency_key VARCHAR(256) NOT NULL,

    request_hash CHAR(64) NOT NULL,

    event_id UUID NOT NULL
        REFERENCES events(id)
        ON DELETE RESTRICT,

    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    expires_at TIMESTAMPTZ,

    CONSTRAINT idempotency_key_unique
        UNIQUE (integration_id, idempotency_key)
);

CREATE INDEX idempotency_expiry_idx
    ON idempotency_keys (expires_at)
    WHERE expires_at IS NOT NULL;