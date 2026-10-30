CREATE TABLE IF NOT EXISTS delivery_state_transitions (
    id BIGSERIAL PRIMARY KEY,

    delivery_id UUID NOT NULL
        REFERENCES deliveries(id)
        ON DELETE CASCADE,

    from_status TEXT
        CHECK (
            from_status IS NULL
            OR from_status IN (
                'queued',
                'processing',
                'succeeded',
                'retry_wait',
                'dead_letter'
            )
        ),

    to_status TEXT NOT NULL
        CHECK (
            to_status IN (
                'queued',
                'processing',
                'succeeded',
                'retry_wait',
                'dead_letter'
            )
        ),

    reason TEXT NOT NULL,

    actor TEXT NOT NULL DEFAULT 'database_trigger',

    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_state_transitions_delivery
    ON delivery_state_transitions (delivery_id, id);


CREATE OR REPLACE FUNCTION hookguard_record_delivery_transition()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
DECLARE
    transition_allowed BOOLEAN := FALSE;
BEGIN
    -- Record the initial state when a delivery is created.
    IF TG_OP = 'INSERT' THEN

        IF NEW.status <> 'queued' THEN
            RAISE EXCEPTION
                'A new delivery must start in queued state';
        END IF;

        INSERT INTO delivery_state_transitions (
            delivery_id,
            from_status,
            to_status,
            reason,
            actor
        )
        VALUES (
            NEW.id,
            NULL,
            NEW.status,
            'delivery_created',
            'database_trigger'
        );

        RETURN NEW;
    END IF;

    -- Record a transition only when the status actually changes.
    IF NEW.status IS DISTINCT FROM OLD.status THEN

        transition_allowed :=
            (
                OLD.status = 'queued'
                AND NEW.status = 'processing'
            )
            OR
            (
                OLD.status = 'processing'
                AND NEW.status IN (
                    'succeeded',
                    'retry_wait',
                    'dead_letter',
                    'queued'
                )
            )
            OR
            (
                OLD.status = 'retry_wait'
                AND NEW.status = 'queued'
            )
            OR
            (
                OLD.status = 'dead_letter'
                AND NEW.status = 'queued'
            );

        IF NOT transition_allowed THEN
            RAISE EXCEPTION
                'Invalid delivery state transition: % -> %',
                OLD.status,
                NEW.status;
        END IF;

        INSERT INTO delivery_state_transitions (
            delivery_id,
            from_status,
            to_status,
            reason,
            actor
        )
        VALUES (
            NEW.id,
            OLD.status,
            NEW.status,
            'status_changed',
            'database_trigger'
        );

    END IF;

    RETURN NEW;
END;
$$;


DROP TRIGGER IF EXISTS trg_delivery_transition_insert
    ON deliveries;

CREATE TRIGGER trg_delivery_transition_insert
AFTER INSERT ON deliveries
FOR EACH ROW
EXECUTE FUNCTION hookguard_record_delivery_transition();


DROP TRIGGER IF EXISTS trg_delivery_transition_update
    ON deliveries;

CREATE TRIGGER trg_delivery_transition_update
AFTER UPDATE OF status ON deliveries
FOR EACH ROW
WHEN (OLD.status IS DISTINCT FROM NEW.status)
EXECUTE FUNCTION hookguard_record_delivery_transition();