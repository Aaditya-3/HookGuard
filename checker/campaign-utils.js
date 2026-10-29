import { Pool } from 'pg';

export function createCheckerPool() {
  const connectionString = process.env.DATABASE_URL;

  if (!connectionString) {
    throw new Error(
      'DATABASE_URL is missing. Configure it in your .env file.',
    );
  }

  return new Pool({
    connectionString,
    max: 5,
    connectionTimeoutMillis: 5000,
    idleTimeoutMillis: 10000,
  });
}

export async function createCheckerIntegration({
  baseUrl,
  adminToken,
  destinationUrl,
  rateLimitPerMinute = 100000,
}) {
  const response = await fetch(`${baseUrl}/api/integrations`, {
    method: 'POST',
    headers: {
      'content-type': 'application/json',
      'x-admin-token': adminToken,
    },
    body: JSON.stringify({
      name: `Correctness campaign ${new Date().toISOString()}`,
      sourceService: 'hookguard-checker',
      eventTypes: ['checker.event'],
      destinationUrl,
      rateLimitPerMinute,
    }),
  });

  const body = await response.json().catch(() => ({}));

  if (!response.ok) {
    throw new Error(
      `Integration creation failed (${response.status}): ` +
      JSON.stringify(body),
    );
  }

  if (!body.integration?.id || !body.integration?.ingressUrl) {
    throw new Error(
      `Integration response is missing required fields: ${JSON.stringify(body)}`,
    );
  }

  return body;
}

export async function sendOperation({
  ingressUrl,
  integrationId,
  key,
  payload,
  eventType = 'checker.event',
}) {
  const rawBody = JSON.stringify(payload);
  const startedAt = Date.now();

  try {
    const response = await fetch(ingressUrl, {
      method: 'POST',
      headers: {
        'content-type': 'application/json',
        'x-event-type': eventType,

        // Send the logical ID in multiple conventional locations
        // so the ingress implementation can apply its configured
        // source-event deduplication behavior.
        'x-event-id': key,
        'x-source-event-id': key,
        'idempotency-key': key,
        'x-idempotency-key': key,
      },
      body: rawBody,
      signal: AbortSignal.timeout(15000),
    });

    const body = await response.json().catch(() => ({}));

    return {
      integrationId,
      idempotencyKey: key,
      startedAt,
      finishedAt: Date.now(),
      statusCode: response.status,
      eventId: body.eventId ?? body.event?.id ?? null,
      deliveryId: body.deliveryId ?? null,
      duplicate: body.duplicate ?? null,
      error: body.error ?? null,
    };
  } catch (error) {
    return {
      integrationId,
      idempotencyKey: key,
      startedAt,
      finishedAt: Date.now(),
      statusCode: null,
      eventId: null,
      deliveryId: null,
      error: error.message,
    };
  }
}

export async function runConcurrent(items, concurrency, task) {
  if (!Number.isInteger(concurrency) || concurrency < 1) {
    throw new Error('Concurrency must be a positive integer');
  }

  const results = new Array(items.length);
  let nextIndex = 0;

  async function runner() {
    while (true) {
      const index = nextIndex++;

      if (index >= items.length) {
        return;
      }

      results[index] = await task(items[index], index);
    }
  }

  const runnerCount = Math.min(concurrency, items.length);

  await Promise.all(
    Array.from({ length: runnerCount }, () => runner()),
  );

  return results;
}

export async function getAudit(pool, integrationId, limit = 25000) {
  const safeLimit = Math.max(1, Math.min(Number(limit) || 25000, 25000));

  const [events, deliveries, attempts] = await Promise.all([
    pool.query(
      `SELECT id, integration_id, source_event_id, event_type, received_at
       FROM events
       WHERE integration_id = $1
       ORDER BY received_at ASC
       LIMIT $2`,
      [integrationId, safeLimit],
    ),

    pool.query(
      `SELECT id, event_id, integration_id, status, attempt_count,
              created_at, updated_at
       FROM deliveries
       WHERE integration_id = $1
       ORDER BY created_at ASC
       LIMIT $2`,
      [integrationId, safeLimit],
    ),

    pool.query(
      `SELECT a.*
       FROM delivery_attempts a
       JOIN deliveries d ON d.id = a.delivery_id
       WHERE d.integration_id = $1
       ORDER BY a.delivery_id, a.started_at ASC
       LIMIT $2`,
      [integrationId, safeLimit],
    ),
  ]);

  return {
    events: events.rows,
    deliveries: deliveries.rows,
    attempts: attempts.rows,

    // This implementation does not query transition history because
    // the current schema does not yet store it.
    transitions: [],
  };
}

export async function waitForTerminalDeliveries({
  pool,
  integrationId,
  expectedCount,
  timeoutMs = 180000,
}) {
  const startedAt = Date.now();
  let audit;

  while (Date.now() - startedAt < timeoutMs) {
    audit = await getAudit(pool, integrationId);

    const terminalCount = audit.deliveries.filter(
      (delivery) =>
        delivery.status === 'succeeded' ||
        delivery.status === 'dead_letter',
    ).length;

    if (
      audit.deliveries.length >= expectedCount &&
      terminalCount >= expectedCount
    ) {
      return audit;
    }

    await new Promise((resolve) => setTimeout(resolve, 1000));
  }

  return audit ?? getAudit(pool, integrationId);
}