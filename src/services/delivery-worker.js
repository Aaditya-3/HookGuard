import { randomUUID } from 'node:crypto';
import { performance } from 'node:perf_hooks';

import { decryptSecret } from './secrets.js';
import { signPayload } from '../lib/signatures.js';
import { validateDestinationUrl } from './destination-validation.js';

const SUCCESSFUL_STATUS_MIN = 200;
const SUCCESSFUL_STATUS_MAX = 299;

function calculateRetryDelayMs(attemptNumber, config) {
  const exponent = Math.min(
    Math.max(attemptNumber - 1, 0),
    20
  );

  const exponentialDelay = Math.min(
    config.retryMaxMs,
    config.retryBaseMs * 2 ** exponent
  );

  // Jitter spreads retries across time.
  const jitteredDelay =
    exponentialDelay * (0.5 + Math.random());

  return Math.floor(
    Math.min(config.retryMaxMs, jitteredDelay)
  );
}

function errorCategory(error, timedOut) {
  if (timedOut) {
    return {
      outcome: 'timeout',
      errorCode: 'DESTINATION_TIMEOUT'
    };
  }

  if (error?.name === 'AbortError') {
    return {
      outcome: 'timeout',
      errorCode: 'DESTINATION_ABORTED'
    };
  }

  if (error?.name === 'AppError') {
    return {
      outcome: 'internal_error',
      errorCode: error.code ?? 'DESTINATION_VALIDATION_FAILED'
    };
  }

  if (error?.name === 'TypeError') {
    return {
      outcome: 'network_error',
      errorCode: 'NETWORK_ERROR'
    };
  }

  return {
    outcome: 'internal_error',
    errorCode: 'DELIVERY_INTERNAL_ERROR'
  };
}

export function createDeliveryWorker({
  pool,
  config,
  logger,
  metrics,
  workerId = `worker-${process.pid}-${randomUUID()}`
}) {
  async function claimDelivery(deliveryId) {
    const client = await pool.connect();

    try {
      await client.query('BEGIN');

      const result = await client.query(
        `
          SELECT
            d.id,
            d.event_id,
            d.integration_id,
            d.destination_url,
            d.status,
            d.attempt_count,
            d.cycle_attempt_count,
            e.event_type,
            e.payload,
            e.received_at,
            i.source_service,
            i.destination_signing_secret_enc
          FROM deliveries d
          JOIN events e
            ON e.id = d.event_id
          JOIN integrations i
            ON i.id = d.integration_id
          WHERE d.id = $1
          FOR UPDATE OF d
        `,
        [deliveryId]
      );

      if (result.rowCount === 0) {
        await client.query('COMMIT');

        logger.warn(
          { deliveryId },
          'Delivery job references an unknown delivery'
        );

        return null;
      }

      const delivery = result.rows[0];

      // A duplicate RabbitMQ message can arrive while another
      // worker is already processing this delivery.
      if (delivery.status !== 'queued') {
        await client.query('COMMIT');

        return null;
      }

      const claimed = await client.query(
        `
          UPDATE deliveries
          SET
            status = 'processing',
            attempt_count = attempt_count + 1,
            cycle_attempt_count = cycle_attempt_count + 1,
            lease_owner = $2,
            lease_expires_at =
              NOW() + (
                $3::double precision
                * INTERVAL '1 millisecond'
              ),
            updated_at = NOW()
          WHERE id = $1
            AND status = 'queued'
          RETURNING
            attempt_count,
            cycle_attempt_count
        `,
        [
          deliveryId,
          workerId,
          config.workerLeaseMs
        ]
      );

      if (claimed.rowCount !== 1) {
        throw new Error(
          'Unable to claim delivery after locking its row'
        );
      }

      const counters = claimed.rows[0];

      const attemptId = randomUUID();

      await client.query(
        `
          INSERT INTO delivery_attempts (
            id,
            delivery_id,
            attempt_number,
            outcome,
            started_at
          )
          VALUES (
            $1,
            $2,
            $3,
            'started',
            NOW()
          )
        `,
        [
          attemptId,
          deliveryId,
          counters.attempt_count
        ]
      );

      await client.query('COMMIT');

      return {
        ...delivery,
        attempt_id: attemptId,
        attempt_count: counters.attempt_count,
        cycle_attempt_count:
          counters.cycle_attempt_count
      };
    } catch (error) {
      await client.query('ROLLBACK').catch(() => {});
      throw error;
    } finally {
      client.release();
    }
  }

  async function finishDelivery({
    delivery,
    outcome,
    httpStatus = null,
    errorCode = null,
    errorMessage = null,
    durationMs
  }) {
    const client = await pool.connect();

    try {
      await client.query('BEGIN');

      const current = await client.query(
        `
          SELECT
            status,
            lease_owner,
            cycle_attempt_count
          FROM deliveries
          WHERE id = $1
          FOR UPDATE
        `,
        [delivery.id]
      );

      if (current.rowCount === 0) {
        throw new Error(
          'Delivery disappeared before completion'
        );
      }

      const state = current.rows[0];

      // The lease may have expired while the HTTP request was
      // in progress. In that case, a scheduler may have already
      // recovered the delivery. The stale worker must not overwrite
      // the new owner's result.
      if (
        state.status !== 'processing' ||
        state.lease_owner !== workerId
      ) {
        await client.query('COMMIT');

        logger.warn(
          {
            deliveryId: delivery.id,
            attemptId: delivery.attempt_id
          },
          'Worker lost delivery ownership before completion'
        );

        return {
          ownershipLost: true
        };
      }

      const success = outcome === 'succeeded';

      let newStatus;
      let nextAttemptAt = null;

      if (success) {
        newStatus = 'succeeded';
      } else if (
        state.cycle_attempt_count >=
        config.maxDeliveryAttempts
      ) {
        newStatus = 'dead_letter';
      } else {
        newStatus = 'retry_wait';

        const delayMs = calculateRetryDelayMs(
          state.cycle_attempt_count,
          config
        );

        nextAttemptAt = delayMs;
      }

      await client.query(
        `
          UPDATE delivery_attempts
          SET
            outcome = $1,
            http_status = $2,
            error_code = $3,
            error_message = $4,
            duration_ms = $5,
            completed_at = NOW()
          WHERE id = $6
            AND delivery_id = $7
            AND outcome = 'started'
        `,
        [
          outcome,
          httpStatus,
          errorCode,
          errorMessage?.slice(0, 1000) ?? null,
          Math.max(0, Math.floor(durationMs)),
          delivery.attempt_id,
          delivery.id
        ]
      );

      await client.query(
        `
          UPDATE deliveries
          SET
            status = $2,
            next_attempt_at = COALESCE(
              NOW() + (
                $3::double precision
                * INTERVAL '1 millisecond'
              ),
              next_attempt_at
            ),
            lease_owner = NULL,
            lease_expires_at = NULL,
            last_error = $4,
            updated_at = NOW()
          WHERE id = $1
        `,
        [
          delivery.id,
          newStatus,
          nextAttemptAt,
          success ? null : errorMessage
        ]
      );

      await client.query('COMMIT');

      return {
        ownershipLost: false,
        status: newStatus
      };
    } catch (error) {
      await client.query('ROLLBACK').catch(() => {});
      throw error;
    } finally {
      client.release();
    }
  }

  async function sendDelivery(delivery) {
    const startedAt = performance.now();

    let outcome = 'internal_error';
    let httpStatus = null;
    let errorCode = null;
    let errorMessage = null;
    let timedOut = false;

    const controller = new AbortController();

    const timeout = setTimeout(() => {
      timedOut = true;

      controller.abort(
        new Error('Destination request timed out')
      );
    }, config.destinationTimeoutMs);

    try {
      // Validate again at delivery time rather than relying
      // exclusively on the check performed during integration creation.
      const destinationUrl = await validateDestinationUrl(
        delivery.destination_url,
        {
          allowPrivate:
            config.allowPrivateDestinations,

          requireHttps:
            config.nodeEnv === 'production'
        }
      );

      const secret = decryptSecret(
        delivery.destination_signing_secret_enc,
        config.encryptionKey
      );

      const envelope = {
        id: delivery.event_id,
        type: delivery.event_type,
        createdAt: delivery.received_at,
        source: delivery.source_service,
        integrationId: delivery.integration_id,
        data: delivery.payload
      };

      const body = JSON.stringify(envelope);

      const timestamp = String(
        Math.floor(Date.now() / 1000)
      );

      const signature = signPayload(
        secret,
        timestamp,
        body
      );

      const response = await fetch(
        destinationUrl,
        {
          method: 'POST',

          redirect: 'manual',

          signal: controller.signal,

          headers: {
            'Content-Type': 'application/json',

            'User-Agent': 'HookGuard/1.0',

            'X-HookGuard-Event-Id':
              delivery.event_id,

            'X-HookGuard-Event-Type':
              delivery.event_type,

            'X-HookGuard-Timestamp': timestamp,

            'X-HookGuard-Signature': signature,

            'Idempotency-Key': delivery.event_id
          },

          body
        }
      );

      httpStatus = response.status;

      // We intentionally do not follow redirects. A redirect could
      // send a request to a different destination.
      await response.body?.cancel().catch(() => {});

      if (
        httpStatus >= SUCCESSFUL_STATUS_MIN &&
        httpStatus <= SUCCESSFUL_STATUS_MAX
      ) {
        outcome = 'succeeded';
      } else {
        outcome = 'http_error';
        errorCode = `HTTP_${httpStatus}`;
        errorMessage =
          `Destination returned HTTP ${httpStatus}`;
      }
    } catch (error) {
      const category = errorCategory(
        error,
        timedOut
      );

      outcome = category.outcome;
      errorCode = category.errorCode;

      errorMessage = timedOut
        ? 'Destination request timed out'
        : String(error.message ?? 'Delivery failed');
    } finally {
      clearTimeout(timeout);
    }

    const durationMs = performance.now() - startedAt;

    metrics.deliveryAttempts.inc({
      outcome
    });

    metrics.deliveryDuration.observe(
      durationMs / 1000
    );

    const result = await finishDelivery({
      delivery,
      outcome,
      httpStatus,
      errorCode,
      errorMessage,
      durationMs
    });

    logger.info(
      {
        deliveryId: delivery.id,
        eventId: delivery.event_id,
        attemptNumber: delivery.attempt_count,
        outcome,
        httpStatus,
        durationMs: Math.round(durationMs),
        ...result
      },
      'Webhook delivery attempt finished'
    );

    return result;
  }

  async function processDelivery(deliveryId) {
    const delivery = await claimDelivery(deliveryId);

    if (!delivery) {
      return {
        processed: false,
        reason: 'not_claimable'
      };
    }

    return sendDelivery(delivery);
  }

  return {
    processDelivery
  };
}