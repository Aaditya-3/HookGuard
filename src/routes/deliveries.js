import { Router } from 'express';
import { randomUUID } from 'node:crypto';
import { z } from 'zod';

import { AppError } from '../lib/errors.js';

export function createDeliveriesRouter({
  pool,
  logger
}) {
  const router = Router();

  router.get('/', async (req, res, next) => {
    try {
      const parsed = z.object({
        integrationId: z.string().uuid().optional(),
        limit: z.coerce.number().int().min(1).max(200).default(50)
      }).safeParse(req.query);

      if (!parsed.success) {
        throw new AppError(
          400,
          'INVALID_QUERY',
          'Delivery query parameters are invalid'
        );
      }

      const { integrationId, limit } = parsed.data;

      const result = await pool.query(
        `
          SELECT
            d.id,
            d.event_id,
            d.integration_id,
            d.destination_url,
            d.status,
            d.attempt_count,
            d.cycle_attempt_count,
            d.replay_count,
            d.next_attempt_at,
            d.last_error,
            d.created_at,
            d.updated_at,
            e.event_type,
            e.received_at
          FROM deliveries d
          JOIN events e ON e.id = d.event_id
          WHERE ($1::uuid IS NULL OR d.integration_id = $1)
          ORDER BY d.created_at DESC
          LIMIT $2
        `,
        [integrationId ?? null, limit]
      );

      return res.status(200).json({
        deliveries: result.rows
      });
    } catch (error) {
      return next(error);
    }
  });

  router.get('/:id', async (req, res, next) => {
    try {
      const parsedId = z.string().uuid().safeParse(
        req.params.id
      );

      if (!parsedId.success) {
        throw new AppError(
          400,
          'INVALID_DELIVERY_ID',
          'Delivery ID must be a UUID'
        );
      }

      const result = await pool.query(
        `
          SELECT
            d.*,
            e.event_type,
            e.payload,
            e.received_at
          FROM deliveries d
          JOIN events e ON e.id = d.event_id
          WHERE d.id = $1
        `,
        [parsedId.data]
      );

      if (result.rowCount === 0) {
        throw new AppError(
          404,
          'DELIVERY_NOT_FOUND',
          'Delivery not found'
        );
      }

      const attempts = await pool.query(
        `
          SELECT
            id,
            attempt_number,
            outcome,
            http_status,
            error_code,
            error_message,
            duration_ms,
            started_at,
            completed_at
          FROM delivery_attempts
          WHERE delivery_id = $1
          ORDER BY attempt_number ASC
        `,
        [parsedId.data]
      );

      return res.status(200).json({
        delivery: result.rows[0],
        attempts: attempts.rows
      });
    } catch (error) {
      return next(error);
    }
  });

  router.post('/:id/replay', async (req, res, next) => {
    const parsedId = z.string().uuid().safeParse(
      req.params.id
    );

    if (!parsedId.success) {
      return next(
        new AppError(
          400,
          'INVALID_DELIVERY_ID',
          'Delivery ID must be a UUID'
        )
      );
    }

    const client = await pool.connect();

    try {
      await client.query('BEGIN');

      const found = await client.query(
        `
          SELECT id, status
          FROM deliveries
          WHERE id = $1
          FOR UPDATE
        `,
        [parsedId.data]
      );

      if (found.rowCount === 0) {
        throw new AppError(
          404,
          'DELIVERY_NOT_FOUND',
          'Delivery not found'
        );
      }

      if (found.rows[0].status !== 'dead_letter') {
        throw new AppError(
          409,
          'DELIVERY_NOT_REPLAYABLE',
          'Only dead-letter deliveries can be replayed'
        );
      }

      const deliveryId = found.rows[0].id;

      await client.query(
        `
          UPDATE deliveries
          SET
            status = 'queued',
            cycle_attempt_count = 0,
            next_attempt_at = NOW(),
            last_error = NULL,
            replay_count = replay_count + 1,
            updated_at = NOW()
          WHERE id = $1
        `,
        [deliveryId]
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
          JSON.stringify({ deliveryId })
        ]
      );

      await client.query('COMMIT');

      logger.info(
        { deliveryId },
        'Manual delivery replay scheduled'
      );

      return res.status(202).json({
        deliveryId,
        status: 'queued'
      });
    } catch (error) {
      await client.query('ROLLBACK').catch(() => {});
      return next(error);
    } finally {
      client.release();
    }
  });

  return router;
}