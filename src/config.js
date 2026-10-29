import 'dotenv/config';

function intEnv(name, fallback, min = 1) {
  const raw = process.env[name];

  const value =
    raw === undefined || raw === ''
      ? fallback
      : Number.parseInt(raw, 10);

  if (!Number.isInteger(value) || value < min) {
    throw new Error(`${name} must be an integer >= ${min}`);
  }

  return value;
}

export function loadConfig() {
  const keyHex = process.env.APP_ENCRYPTION_KEY ?? '';

  if (!/^[a-fA-F0-9]{64}$/.test(keyHex)) {
    throw new Error(
      'APP_ENCRYPTION_KEY must be 64 hexadecimal characters ' +
      '(generate with: openssl rand -hex 32)'
    );
  }

  const adminToken = process.env.ADMIN_API_TOKEN;

  if (!adminToken || adminToken.length < 16) {
    throw new Error(
      'ADMIN_API_TOKEN must be set and contain at least 16 characters'
    );
  }

  const ingressBaseUrl =
    process.env.INGRESS_BASE_URL ?? 'http://localhost:3000';

  let parsedBase;

  try {
    parsedBase = new URL(ingressBaseUrl);
  } catch {
    throw new Error('INGRESS_BASE_URL must be a valid URL');
  }

  if (!['http:', 'https:'].includes(parsedBase.protocol)) {
    throw new Error('INGRESS_BASE_URL must use HTTP or HTTPS');
  }

  const destinationTimeoutMs = intEnv(
    'DESTINATION_TIMEOUT_MS',
    10000
  );

  const workerLeaseMs = intEnv(
    'WORKER_LEASE_MS',
    60000
  );

  if (workerLeaseMs <= destinationTimeoutMs) {
    throw new Error(
      'WORKER_LEASE_MS must be greater than DESTINATION_TIMEOUT_MS'
    );
  }

  return Object.freeze({
    nodeEnv: process.env.NODE_ENV ?? 'development',

    port: intEnv('PORT', 3000),

    logLevel: process.env.LOG_LEVEL ?? 'info',

    databaseUrl:
      process.env.DATABASE_URL ??
      'postgresql://hookguard:hookguard_local_only_change_me@localhost:5432/hookguard',

    rabbitmqUrl:
      process.env.RABBITMQ_URL ??
      'amqp://hookguard:hookguard_rabbit_local_change_me@localhost:5672',

    redisUrl:
      process.env.REDIS_URL ??
      'redis://:hookguard_redis_local_change_me@localhost:6379',

    encryptionKey: Buffer.from(keyHex, 'hex'),

    adminApiToken: adminToken,

    ingressBaseUrl: ingressBaseUrl.replace(/\/$/, ''),

    allowPrivateDestinations:
      process.env.ALLOW_PRIVATE_DESTINATIONS === 'true' &&
      (process.env.NODE_ENV ?? 'development') !== 'production',

    ingressRateLimitPerMinute: intEnv(
      'INGRESS_RATE_LIMIT_PER_MINUTE',
      1200
    ),

    destinationTimeoutMs,

    maxDeliveryAttempts: intEnv(
      'MAX_DELIVERY_ATTEMPTS',
      8
    ),

    retryBaseMs: intEnv('RETRY_BASE_MS', 1000),

    retryMaxMs: intEnv('RETRY_MAX_MS', 60000),

    workerConcurrency: intEnv(
      'WORKER_CONCURRENCY',
      20
    ),

    workerLeaseMs,

    outboxPollMs: intEnv('OUTBOX_POLL_MS', 300),

    schedulerPollMs: intEnv('SCHEDULER_POLL_MS', 1000),

    workerMetricsPort: intEnv(
      'WORKER_METRICS_PORT',
      9101
    )
  });
}