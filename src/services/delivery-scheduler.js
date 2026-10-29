import { randomUUID } from 'node:crypto';

async function insertDeliveryOutbox(
  client,
  deliveryId
) {
  await client.query(
    `
      INSERT INTO outbox_messages (
        id,
        topic,
        aggregate_id,
        payload,
        attempts,
        created_at,
        next_attempt_at
      )
      VALUES (
        $1,
        'delivery.requested',
        $2,
        $3::jsonb,
        0,
        NOW(),
        NOW()
      )
    `,
    [
      randomUUID(),
      deliveryId,
      JSON.stringify({ deliveryId })
    ]
  );
}

export async function scheduleDueDeliveries({
  pool,
  config,
  logger,
  batchSize = 100
}) {
  const client = await pool.connect();

  let scheduledRetries = 0;
  let recoveredWorkers = 0;
  let deadLettered = 0;

  try {
    await client.query('BEGIN');

    // ------------------------------------------------------
    // 1. Find retry_wait deliveries whose retry time has come.
    // ------------------------------------------------------

    const dueRetries = await client.query(
      `
        SELECT id
        FROM deliveries
        WHERE status = 'retry_wait'
          AND next_attempt_at <= NOW()
        ORDER BY next_attempt_at
        FOR UPDATE SKIP LOCKED
        LIMIT $1
      `,
      [batchSize]
    );

    for (const row of dueRetries.rows) {
      await client.query(
        `
          UPDATE deliveries
          SET
            status = 'queued',
            updated_at = NOW()
          WHERE id = $1
            AND status = 'retry_wait'
        `,
        [row.id]
      );

      await insertDeliveryOutbox(
        client,
        row.id
      );

      scheduledRetries += 1;
    }

    // ------------------------------------------------------
    // 2. Find processing deliveries with expired leases.
    // ------------------------------------------------------

    const expiredLeases = await client.query(
      `
        SELECT
          id,
          attempt_count,
          cycle_attempt_count
        FROM deliveries
        WHERE status = 'processing'
          AND lease_expires_at <= NOW()
        ORDER BY lease_expires_at
        FOR UPDATE SKIP LOCKED
        LIMIT $1
      `,
      [batchSize]
    );

    for (const row of expiredLeases.rows) {
      // The worker disappeared before recording a final result.
      // Close the corresponding attempt as an interrupted attempt.
      await client.query(
        `
          UPDATE delivery_attempts
          SET
            outcome = 'internal_error',
            error_code = 'WORKER_LEASE_EXPIRED',
            error_message =
              'Worker lease expired before attempt completion',
            completed_at = NOW()
          WHERE delivery_id = $1
            AND attempt_number = $2
            AND outcome = 'started'
        `,
        [
          row.id,
          row.attempt_count
        ]
      );

      if (
        row.cycle_attempt_count >=
        config.maxDeliveryAttempts
      ) {
        // The configured retry budget has been exhausted.
        await client.query(
          `
            UPDATE deliveries
            SET
              status = 'dead_letter',
              lease_owner = NULL,
              lease_expires_at = NULL,
              last_error = 'Worker lease expired after retry budget was exhausted',
              updated_at = NOW()
            WHERE id = $1
              AND status = 'processing'
          `,
          [row.id]
        );

        deadLettered += 1;

        continue;
      }

      // Requeue the delivery. The worker will claim it again.
      await client.query(
        `
          UPDATE deliveries
          SET
            status = 'queued',
            lease_owner = NULL,
            lease_expires_at = NULL,
            next_attempt_at = NOW(),
            last_error = 'Worker lease expired; delivery recovered',
            updated_at = NOW()
          WHERE id = $1
            AND status = 'processing'
        `,
        [row.id]
      );

      await insertDeliveryOutbox(
        client,
        row.id
      );

      recoveredWorkers += 1;
    }

    await client.query('COMMIT');

    if (
      scheduledRetries > 0 ||
      recoveredWorkers > 0 ||
      deadLettered > 0
    ) {
      logger.info(
        {
          scheduledRetries,
          recoveredWorkers,
          deadLettered
        },
        'Delivery scheduler processed pending work'
      );
    }

    return {
      scheduledRetries,
      recoveredWorkers,
      deadLettered
    };
  } catch (error) {
    await client.query('ROLLBACK').catch(() => {});
    throw error;
  } finally {
    client.release();
  }
}