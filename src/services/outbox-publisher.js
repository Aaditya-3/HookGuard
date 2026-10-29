import amqp from 'amqplib';
import { randomUUID } from 'node:crypto';

const DELIVERY_QUEUE = 'hookguard.delivery';
const DEAD_LETTER_EXCHANGE = 'hookguard.delivery.dlx';
const DEAD_LETTER_QUEUE = 'hookguard.delivery.dlq';
const DEAD_LETTER_ROUTING_KEY = 'delivery.dead';

const BATCH_SIZE = 100;
const OUTBOX_LEASE_MS = 60000;

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function retryDelayMs(attempt, config) {
  const exponent = Math.min(
    Math.max(attempt - 1, 0),
    20
  );

  const baseDelay = Math.min(
    config.retryMaxMs,
    config.retryBaseMs * 2 ** exponent
  );

  // Jitter spreads retries over time instead of
  // making every publisher retry simultaneously.
  return Math.floor(
    baseDelay * (0.5 + Math.random())
  );
}

export function createOutboxPublisher({
  pool,
  config,
  logger
}) {
  let connection = null;
  let channel = null;

  let stopped = true;
  let loopPromise = null;

  async function ensureChannel() {
    if (channel && connection) {
      return channel;
    }

    connection = await amqp.connect(
      config.rabbitmqUrl
    );

    const activeConnection = connection;

    connection.on('error', (error) => {
      logger.error(
        { err: error },
        'RabbitMQ connection error in outbox publisher'
      );
    });

    connection.on('close', () => {
      if (connection === activeConnection) {
        connection = null;
        channel = null;
      }

      logger.warn(
        'RabbitMQ publisher connection closed'
      );
    });

    channel = await connection.createConfirmChannel();

    channel.on('error', (error) => {
      logger.error(
        { err: error },
        'RabbitMQ confirm channel error'
      );
    });

    await channel.assertExchange(
      DEAD_LETTER_EXCHANGE,
      'direct',
      { durable: true }
    );

    await channel.assertQueue(
      DEAD_LETTER_QUEUE,
      { durable: true }
    );

    await channel.bindQueue(
      DEAD_LETTER_QUEUE,
      DEAD_LETTER_EXCHANGE,
      DEAD_LETTER_ROUTING_KEY
    );

    await channel.assertQueue(
      DELIVERY_QUEUE,
      {
        durable: true,
        arguments: {
          'x-dead-letter-exchange':
            DEAD_LETTER_EXCHANGE,

          'x-dead-letter-routing-key':
            DEAD_LETTER_ROUTING_KEY
        }
      }
    );

    return channel;
  }

  async function claimBatch() {
    const owner =
      `outbox-${process.pid}-${randomUUID()}`;

    const client = await pool.connect();

    try {
      await client.query('BEGIN');

      const result = await client.query(
        `
          WITH candidates AS (
            SELECT id
            FROM outbox_messages
            WHERE published_at IS NULL
              AND next_attempt_at <= NOW()
              AND (
                locked_until IS NULL
                OR locked_until <= NOW()
              )
            ORDER BY created_at
            FOR UPDATE SKIP LOCKED
            LIMIT $1
          )
          UPDATE outbox_messages AS o
          SET
            locked_by = $2,
            locked_until =
              NOW() + (
                $3::double precision
                * INTERVAL '1 millisecond'
              ),
            attempts = o.attempts + 1
          FROM candidates
          WHERE o.id = candidates.id
          RETURNING
            o.id,
            o.topic,
            o.aggregate_id,
            o.payload,
            o.attempts
        `,
        [
          BATCH_SIZE,
          owner,
          OUTBOX_LEASE_MS
        ]
      );

      await client.query('COMMIT');

      return {
        owner,
        rows: result.rows
      };
    } catch (error) {
      await client.query('ROLLBACK').catch(() => {});
      throw error;
    } finally {
      client.release();
    }
  }

  async function markPublished(rows, owner) {
    if (rows.length === 0) {
      return;
    }

    const ids = rows.map((row) => row.id);

    await pool.query(
      `
        UPDATE outbox_messages
        SET
          published_at = NOW(),
          locked_by = NULL,
          locked_until = NULL,
          last_error = NULL
        WHERE id = ANY($1::uuid[])
          AND locked_by = $2
          AND published_at IS NULL
      `,
      [ids, owner]
    );
  }

  async function schedulePublicationRetry(
    rows,
    owner,
    error
  ) {
    for (const row of rows) {
      const delayMs = retryDelayMs(
        row.attempts,
        config
      );

      await pool.query(
        `
          UPDATE outbox_messages
          SET
            next_attempt_at =
              NOW() + (
                $1::double precision
                * INTERVAL '1 millisecond'
              ),
            locked_by = NULL,
            locked_until = NULL,
            last_error = $2
          WHERE id = $3
            AND locked_by = $4
            AND published_at IS NULL
        `,
        [
          delayMs,
          String(error.message).slice(0, 1000),
          row.id,
          owner
        ]
      );
    }
  }

  async function pollOnce() {
    const activeChannel = await ensureChannel();

    const { owner, rows } = await claimBatch();

    if (rows.length === 0) {
      return;
    }

    try {
      for (const row of rows) {
        const message = {
          outboxId: row.id,
          topic: row.topic,
          aggregateId: row.aggregate_id,
          payload: row.payload
        };

        activeChannel.sendToQueue(
          DELIVERY_QUEUE,
          Buffer.from(JSON.stringify(message)),
          {
            persistent: true,
            contentType: 'application/json',
            messageId: row.id,
            type: row.topic,
            timestamp: Date.now()
          }
        );
      }

      // Wait for broker publisher confirms before marking
      // the corresponding outbox rows as published.
      await activeChannel.waitForConfirms();

      await markPublished(rows, owner);

      logger.info(
        {
          count: rows.length
        },
        'Outbox batch published'
      );
    } catch (error) {
      logger.error(
        {
          err: error,
          count: rows.length
        },
        'Outbox publication failed'
      );

      await schedulePublicationRetry(
        rows,
        owner,
        error
      ).catch((retryError) => {
        logger.error(
          { err: retryError },
          'Unable to reschedule failed outbox batch'
        );
      });

      // Force a fresh connection on the next cycle.
      if (channel === activeChannel) {
        channel = null;
        connection = null;
      }

      throw error;
    }
  }

  async function runLoop() {
    while (!stopped) {
      try {
        await pollOnce();
      } catch (error) {
        logger.warn(
          { err: error },
          'Outbox polling cycle failed'
        );
      }

      if (!stopped) {
        await sleep(config.outboxPollMs);
      }
    }
  }

  function start() {
    if (loopPromise) {
      return;
    }

    stopped = false;
    loopPromise = runLoop();
  }

  async function stop() {
    stopped = true;

    if (loopPromise) {
      await loopPromise;
      loopPromise = null;
    }

    const currentChannel = channel;
    const currentConnection = connection;

    channel = null;
    connection = null;

    await currentChannel?.close().catch(() => {});
    await currentConnection?.close().catch(() => {});
  }

  return {
    start,
    stop,
    pollOnce
  };
}