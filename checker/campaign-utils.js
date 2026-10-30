import 'dotenv/config';
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
    }),
  });

  const body = await response.json().catch(() => ({}));

  if (!response.ok) {
    const message =
      body?.error?.message ??
      body?.message ??
      `HTTP ${response.status}`;

    throw new Error(
      `Integration creation failed (${response.status}): ${message}`,
    );
  }

  // The API returns ingressUrl at the top level, not inside integration.
  const ingressUrl =
    body.ingressUrl ?? body.integration?.ingressUrl;

  if (!body.integration?.id || !ingressUrl) {
    throw new Error(
      'Invalid integration response. ' +
      `Has integration ID: ${Boolean(body.integration?.id)}, ` +
      `has ingress URL: ${Boolean(ingressUrl)}, ` +
      `response keys: ${Object.keys(body).join(', ')}`,
    );
  }

  // Normalize the response so callers can access integration.ingressUrl.
  return {
    ...body,
    integration: {
      ...body.integration,
      ingressUrl,
    },
  };
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
    throw new Error('Concurrency must be a positive integer.');
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
    Array.from(
      { length: runnerCount },
      () => runner(),
    ),
  );

  return results;
}

export async function getAudit(pool, integrationId, limit = 25000) {
  const safeLimit = Math.max(
    1,
    Math.min(Number(limit) || 25000, 25000),
  );

  const [events, deliveries, attempts, transitions] =
    await Promise.all([
      pool.query(
        `SELECT id, integration_id, source_event_id,
                event_type, received_at
         FROM events
         WHERE integration_id = $1
         ORDER BY received_at ASC
         LIMIT $2`,
        [integrationId, safeLimit],
      ),

      pool.query(
        `SELECT id, event_id, integration_id, status,
                attempt_count, created_at, updated_at
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

      pool.query(
        `SELECT
             t.id,
             t.delivery_id,
             t.from_status,
             t.to_status,
             t.reason,
             t.actor,
             t.created_at
         FROM delivery_state_transitions t
         JOIN deliveries d ON d.id = t.delivery_id
         WHERE d.integration_id = $1
         ORDER BY t.id ASC
         LIMIT $2`,
        [integrationId, safeLimit],
      ),
    ]);

  return {
    events: events.rows,
    deliveries: deliveries.rows,
    attempts: attempts.rows,
    transitions: transitions.rows,
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