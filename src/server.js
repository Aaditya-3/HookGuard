import { loadConfig } from './config.js';
import { createPool } from './db/pool.js';
import { createApp } from './app.js';
import { logger } from './logger.js';
import { createMetrics } from './metrics.js';

import Redis from 'ioredis';

const config = loadConfig();

const pool = createPool(config, logger);

const redis = new Redis(
  config.redisUrl,
  {
    maxRetriesPerRequest: 1,
    enableOfflineQueue: false,
    lazyConnect: false
  }
);

const metrics = createMetrics('api');

const app = createApp({
  pool,
  redis,
  config,
  logger,
  metrics
});

const server = app.listen(
  config.port,
  () => {
    logger.info(
      { port: config.port },
      'HookGuard API started'
    );
  }
);

async function shutdown(signal) {
  logger.info(
    { signal },
    'Shutting down API'
  );

  server.close(async () => {
    await redis.quit().catch(() => {});
    await pool.end().catch(() => {});

    process.exit(0);
  });

  setTimeout(
    () => process.exit(1),
    10000
  ).unref();
}

process.on(
  'SIGTERM',
  () => shutdown('SIGTERM')
);

process.on(
  'SIGINT',
  () => shutdown('SIGINT')
);