import amqp from 'amqplib';

import { createServer } from 'node:http';

import { loadConfig } from './config.js';
import { createPool } from './db/pool.js';
import { logger } from './logger.js';
import { createMetrics } from './metrics.js';

import {
  createOutboxPublisher
} from './services/outbox-publisher.js';

import {
  scheduleDueDeliveries
} from './services/delivery-scheduler.js';

import {
  createDeliveryWorker
} from './services/delivery-worker.js';

const DELIVERY_QUEUE = 'hookguard.delivery';
const DEAD_LETTER_EXCHANGE = 'hookguard.delivery.dlx';
const DEAD_LETTER_QUEUE = 'hookguard.delivery.dlq';
const DEAD_LETTER_ROUTING_KEY = 'delivery.dead';

const config = loadConfig();

const pool = createPool(config, logger);

const metrics = createMetrics('worker');

const deliveryWorker = createDeliveryWorker({
  pool,
  config,
  logger,
  metrics
});

const outboxPublisher = createOutboxPublisher({
  pool,
  config,
  logger
});

let stopping = false;

let consumerConnection = null;
let consumerChannel = null;

let schedulerBusy = false;

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function startMetricsServer() {
  const server = createServer(async (req, res) => {
    if (req.url !== '/metrics') {
      res.writeHead(404, {
        'Content-Type': 'text/plain'
      });

      res.end('Not found');

      return;
    }

    try {
      res.writeHead(200, {
        'Content-Type': metrics.registry.contentType
      });

      res.end(
        await metrics.registry.metrics()
      );
    } catch (error) {
      logger.error(
        { err: error },
        'Unable to expose worker metrics'
      );

      res.writeHead(500);
      res.end('Metrics unavailable');
    }
  });

  server.listen(
    config.workerMetricsPort,
    '0.0.0.0',
    () => {
      logger.info(
        {
          port: config.workerMetricsPort
        },
        'Worker metrics server started'
      );
    }
  );

  return server;
}

async function runSchedulerOnce() {
  if (schedulerBusy || stopping) {
    return;
  }

  schedulerBusy = true;

  try {
    const result = await scheduleDueDeliveries({
      pool,
      config,
      logger
    });

    if (
      result.scheduledRetries > 0 ||
      result.recoveredWorkers > 0 ||
      result.deadLettered > 0
    ) {
      logger.info(
        result,
        'Recovery scheduler completed'
      );
    }
  } catch (error) {
    logger.error(
      { err: error },
      'Recovery scheduler failed'
    );
  } finally {
    schedulerBusy = false;
  }
}

async function handleMessage(channel, message) {
  if (!message) {
    return;
  }

  try {
    const decoded = JSON.parse(
      message.content.toString('utf8')
    );

    const deliveryId =
      decoded?.payload?.deliveryId;

    if (
      typeof deliveryId !== 'string' ||
      !/^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(
        deliveryId
      )
    ) {
      logger.error(
        {
          messageId: message.properties.messageId
        },
        'Malformed delivery job sent to dead-letter queue'
      );

      channel.nack(
        message,
        false,
        false
      );

      return;
    }

    await deliveryWorker.processDelivery(
      deliveryId
    );

    // Ack only after processing has reached a durable outcome,
    // or the worker has determined that the delivery is no
    // longer eligible for this duplicate message.
    channel.ack(message);
  } catch (error) {
    logger.error(
      {
        err: error,
        messageId: message.properties.messageId
      },
      'Delivery job processing failed'
    );

    // A database or unexpected processing error should not
    // silently discard the job. Requeue it for later processing.
    await sleep(500);

    try {
      channel.nack(
        message,
        false,
        true
      );
    } catch {
      // The channel may have closed while processing.
      // The broker will requeue the unacknowledged message.
    }
  }
}

async function runConsumerLoop() {
  while (!stopping) {
    let connection = null;
    let channel = null;

    try {
      logger.info(
        'Connecting worker to RabbitMQ'
      );

      connection = await amqp.connect(
        config.rabbitmqUrl
      );

      consumerConnection = connection;

      connection.on('error', (error) => {
        logger.error(
          { err: error },
          'RabbitMQ worker connection error'
        );
      });

      channel = await connection.createChannel();

      consumerChannel = channel;

      channel.on('error', (error) => {
        logger.error(
          { err: error },
          'RabbitMQ worker channel error'
        );
      });

      await channel.assertExchange(
        DEAD_LETTER_EXCHANGE,
        'direct',
        {
          durable: true
        }
      );

      await channel.assertQueue(
        DEAD_LETTER_QUEUE,
        {
          durable: true
        }
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

      // Bound the number of unacknowledged messages assigned
      // to this consumer.
      await channel.prefetch(
        config.workerConcurrency
      );

      await channel.consume(
        DELIVERY_QUEUE,
        (message) => {
          void handleMessage(
            channel,
            message
          );
        },
        {
          noAck: false
        }
      );

      logger.info(
        {
          concurrency: config.workerConcurrency
        },
        'RabbitMQ worker consuming delivery jobs'
      );

      // Wait until RabbitMQ closes the connection.
      await new Promise((resolve) => {
        connection.once('close', resolve);
      });
    } catch (error) {
      if (!stopping) {
        logger.error(
          { err: error },
          'RabbitMQ consumer failed'
        );
      }
    } finally {
      if (consumerChannel === channel) {
        consumerChannel = null;
      }

      if (consumerConnection === connection) {
        consumerConnection = null;
      }

      await channel?.close().catch(() => {});
      await connection?.close().catch(() => {});
    }

    if (!stopping) {
      logger.warn(
        'Retrying RabbitMQ connection in 2 seconds'
      );

      await sleep(2000);
    }
  }
}

const metricsServer = startMetricsServer();

const schedulerInterval = setInterval(() => {
  void runSchedulerOnce();
}, config.schedulerPollMs);

schedulerInterval.unref();

outboxPublisher.start();

void runSchedulerOnce();

const consumerLoop = runConsumerLoop();

async function shutdown(signal) {
  if (stopping) {
    return;
  }

  stopping = true;

  logger.info(
    { signal },
    'Shutting down worker'
  );

  clearInterval(schedulerInterval);

  await outboxPublisher.stop().catch((error) => {
    logger.error(
      { err: error },
      'Error stopping outbox publisher'
    );
  });

  await consumerChannel?.close().catch(() => {});
  await consumerConnection?.close().catch(() => {});

  await consumerLoop.catch(() => {});

  await new Promise((resolve) => {
    metricsServer.close(resolve);
  });

  await pool.end();

  logger.info('Worker shutdown complete');
}

process.on('SIGINT', () => {
  void shutdown('SIGINT');
});

process.on('SIGTERM', () => {
  void shutdown('SIGTERM');
});