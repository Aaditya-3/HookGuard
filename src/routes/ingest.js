import { Router } from 'express';
import { createHash, randomUUID } from 'node:crypto';

import { AppError } from '../lib/errors.js';

import { hashToken } from '../services/secrets.js';

import {
  consumeRateLimit
} from '../services/rate-limiter.js';

export function createIngestionRouter({
  pool,
  redis,
  config,
  metrics,
  logger
}) {
  const router = Router();

  router.post('/:token', async (req, res, next) => {
    const token = req.params.token;

    if (!/^[A-Za-z0-9_-]{43}$/.test(token)) {
      metrics.ingressRequests.inc({
        outcome: 'invalid_token'
      });

      return res.status(404).json({
        error: {
          code: 'INGRESS_NOT_FOUND',
          message: 'Ingress endpoint not found'
        }
      });
    }

    const tokenHash = hashToken(token);

    // Rate-limit by source IP and ingress capability.
    const ip = req.ip ?? req.socket.remoteAddress ?? 'unknown';

    let ipLimit;
    let tokenLimit;

    try {
      ipLimit = await consumeRateLimit(
        redis,
        `hookguard:ingress:ip:${createHash('sha256').update(ip).digest('hex')}`,
        config.ingressRateLimitPerMinute,
        60
      );

      tokenLimit = await consumeRateLimit(
        redis,
        `hookguard:ingress:token:${tokenHash}`,
        config.ingressRateLimitPerMinute,
        60
      );
    } catch (error) {
      logger.error(
        { err: error },
        'Ingress rate limiter unavailable'
      );

      return next(
        new AppError(
          503,
          'RATE_LIMITER_UNAVAILABLE',
          'Webhook ingestion is temporarily unavailable'
        )
      );
    }

    if (!ipLimit.allowed || !tokenLimit.allowed) {
      metrics.ingressRequests.inc({
        outcome: 'rate_limited'
      });

      res.set(
        'Retry-After',
        String(
          Math.max(
            ipLimit.retryAfterSeconds,
            tokenLimit.retryAfterSeconds
          )
        )
      );

      return res.status(429).json({
        error: {
          code: 'RATE_LIMITED',
          message: 'Too many webhook requests'
        }
      });
    }

    try {
      if (
        !req.body ||
        typeof req.body !== 'object' ||
        Array.isArray(req.body)
      ) {
        throw new AppError(
          400,
          'INVALID_EVENT',
          'Webhook body must be a JSON object'
        );
      }

      const eventType =
        req.body.type ?? req.body.event;

      if (
        typeof eventType !== 'string' ||
        eventType.trim().length === 0 ||
        eventType.length > 200
      ) {
        throw new AppError(
          400,
          'INVALID_EVENT_TYPE',
          'A valid event type is required'
        );
      }

      const integrationResult = await pool.query(
        `
          SELECT
            id,
            source_service,
            subscribed_event_types,
            destination_url,
            status
          FROM integrations
          WHERE ingress_token_hash = $1
        `,
        [tokenHash]
      );

      if (integrationResult.rowCount === 0) {
        metrics.ingressRequests.inc({
          outcome: 'unknown_ingress'
        });

        return res.status(404).json({
          error: {
            code: 'INGRESS_NOT_FOUND',
            message: 'Ingress endpoint not found'
          }
        });
      }

      const integration = integrationResult.rows[0];

      if (integration.status !== 'active') {
        metrics.ingressRequests.inc({
          outcome: 'integration_disabled'
        });

        return res.status(410).json({
          error: {
            code: 'INTEGRATION_DISABLED',
            message: 'This integration is disabled'
          }
        });
      }

      if (
        !integration.subscribed_event_types.includes(
          eventType
        )
      ) {
        metrics.ingressRequests.inc({
          outcome: 'filtered'
        });

        return res.status(202).json({
          status: 'ignored',
          reason: 'event_type_not_subscribed'
        });
      }

      const sourceEventId =
        req.get('x-source-event-id')?.trim() || null;

      if (
        sourceEventId &&
        sourceEventId.length > 256
      ) {
        throw new AppError(
          400,
          'INVALID_SOURCE_EVENT_ID',
          'Source event ID exceeds 256 characters'
        );
      }

      const client = await pool.connect();

      try {
        await client.query('BEGIN');

        const eventId = randomUUID();

        const insertedEvent = await client.query(
          `
            INSERT INTO events (
              id,
              integration_id,
              source_event_id,
              event_type,
              payload,
              received_at
            )
            VALUES ($1, $2, $3, $4, $5::jsonb, NOW())
            ON CONFLICT (integration_id, source_event_id)
            DO NOTHING
            RETURNING id
          `,
          [
            eventId,
            integration.id,
            sourceEventId,
            eventType,
            JSON.stringify(req.body)
          ]
        );

        if (insertedEvent.rowCount === 0) {
          const existingEvent = await client.query(
            `
              SELECT
                id,
                event_type,
                payload = $3::jsonb AS same_payload
              FROM events
              WHERE integration_id = $1
                AND source_event_id = $2
            `,
            [
              integration.id,
              sourceEventId,
              JSON.stringify(req.body)
            ]
          );

          if (existingEvent.rowCount === 0) {
            throw new Error(
              'Duplicate event disappeared during lookup'
            );
          }

          const existing = existingEvent.rows[0];

          if (
            existing.event_type !== eventType ||
            !existing.same_payload
          ) {
            throw new AppError(
              409,
              'IDEMPOTENCY_CONFLICT',
              'The source event ID was reused with different content'
            );
          }

          const existingDelivery = await client.query(
            `
              SELECT id, status
              FROM deliveries
              WHERE event_id = $1
            `,
            [existing.id]
          );

          await client.query('COMMIT');

          metrics.ingressRequests.inc({
            outcome: 'duplicate'
          });

          return res.status(202).json({
            status: 'duplicate',
            eventId: existing.id,
            delivery: existingDelivery.rows[0] ?? null
          });
        }

        const deliveryId = randomUUID();

        await client.query(
          `
            INSERT INTO deliveries (
              id,
              event_id,
              integration_id,
              destination_url,
              status,
              attempt_count,
              cycle_attempt_count,
              replay_count,
              next_attempt_at,
              created_at,
              updated_at
            )
            VALUES (
              $1, $2, $3, $4,
              'queued', 0, 0, 0,
              NOW(), NOW(), NOW()
            )
          `,
          [
            deliveryId,
            eventId,
            integration.id,
            integration.destination_url
          ]
        );

        await client.query(
          `
            INSERT INTO outbox_messages (
              id,
              topic,
              aggregate_id,
              payload,
              attempts,
              created_at
            )
            VALUES (
              $1,
              'delivery.requested',
              $2,
              $3::jsonb,
              0,
              NOW()
            )
          `,
          [
            randomUUID(),
            deliveryId,
            JSON.stringify({
              deliveryId
            })
          ]
        );

        await client.query('COMMIT');

        metrics.ingressRequests.inc({
          outcome: 'accepted'
        });

        logger.info(
          {
            requestId: req.requestId,
            integrationId: integration.id,
            eventId,
            deliveryId
          },
          'Webhook durably accepted'
        );

        return res.status(202).json({
          status: 'queued',
          eventId,
          deliveryId
        });
      } catch (error) {
        await client.query('ROLLBACK').catch(() => {});
        throw error;
      } finally {
        client.release();
      }
    } catch (error) {
      if (error instanceof AppError) {
        return next(error);
      }

      logger.error(
        {
          err: error,
          requestId: req.requestId
        },
        'Webhook ingestion failed'
      );

      return next(
        new AppError(
          503,
          'INGESTION_FAILED',
          'Webhook ingestion could not be completed'
        )
      );
    }
  });

  return router;
}