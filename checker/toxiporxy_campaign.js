import 'dotenv/config';

import { execFile } from 'node:child_process';
import { promisify } from 'node:util';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

import { checkHistory } from './history-checker.js';

import {
  createCheckerIntegration,
  createCheckerPool,
  getAudit,
  sendOperation,
  waitForTerminalDeliveries,
} from './campaign-utils.js';

const execFileAsync = promisify(execFile);

const here = path.dirname(fileURLToPath(import.meta.url));
const projectRoot = path.resolve(here, '..');

const baseUrl = (
  process.env.HOOKGUARD_BASE_URL ?? 'http://127.0.0.1:3000'
).replace(/\/$/, '');

const adminToken = process.env.ADMIN_API_TOKEN;

const toxiproxyUrl = (
  process.env.TOXIPROXY_API_URL ?? 'http://127.0.0.1:8474'
).replace(/\/$/, '');

const proxyName = 'hookguard-sink';
const toxicName = 'response-latency';

const destinationUrl =
  process.env.CHECKER_TOXIPROXY_DESTINATION_URL ??
  'http://toxiproxy:8666/webhook';

const pool = createCheckerPool();

async function apiRequest(path, options = {}) {
  const response = await fetch(`${toxiproxyUrl}${path}`, {
    ...options,
    headers: {
      'content-type': 'application/json',
      ...options.headers,
    },
    signal: AbortSignal.timeout(5000),
  });

  const text = await response.text();

  if (!response.ok) {
    throw new Error(
      `Toxiproxy API error (${response.status}): ${text}`,
    );
  }

  return text ? JSON.parse(text) : null;
}

async function removeToxic() {
  try {
    await apiRequest(
      `/proxies/${proxyName}/toxics/${toxicName}`,
      {
        method: 'DELETE',
      },
    );
  } catch (error) {
    // The toxic may already have been removed or may never have
    // been installed. Only log this during cleanup.
    console.warn(`Toxic cleanup: ${error.message}`);
  }
}

async function setSinkMode(mode) {
  const response = await fetch(
    'http://127.0.0.1:4001/__control',
    {
      method: 'POST',
      headers: {
        'content-type': 'application/json',
      },
      body: JSON.stringify({ mode }),
      signal: AbortSignal.timeout(5000),
    },
  );

  if (!response.ok) {
    throw new Error(
      `Could not configure sink: HTTP ${response.status}`,
    );
  }
}

async function waitForRetry(integrationId, timeoutMs = 60000) {
  const deadline = Date.now() + timeoutMs;

  while (Date.now() < deadline) {
    const result = await pool.query(
      `SELECT id, status, attempt_count
       FROM deliveries
       WHERE integration_id = $1
       ORDER BY created_at DESC
       LIMIT 1`,
      [integrationId],
    );

    const delivery = result.rows[0];

    if (
      delivery?.status === 'retry_wait' ||
      delivery?.status === 'dead_letter' ||
      delivery?.status === 'succeeded'
    ) {
      return delivery;
    }

    await new Promise((resolve) => setTimeout(resolve, 250));
  }

  return null;
}

async function main() {
  if (!adminToken) {
    throw new Error('ADMIN_API_TOKEN is missing from .env.');
  }

  console.log('Starting Toxiproxy network-failure campaign.');

  // Ensure the named proxy exists before creating an integration.
  await execFileAsync(
    process.execPath,
    ['scripts/setup-toxiproxy.js'],
    { cwd: projectRoot },
  );

  const created = await createCheckerIntegration({
    baseUrl,
    adminToken,
    destinationUrl,
  });

  const integration = created.integration;
  const key = `toxiproxy-${Date.now()}`;

  let operation;
  let toxicInstalled = false;

  try {
    await setSinkMode('success');

    await apiRequest(`/proxies/${proxyName}/toxics`, {
      method: 'POST',
      body: JSON.stringify({
        name: toxicName,
        type: 'latency',
        stream: 'downstream',
        toxicity: 1.0,
        attributes: {
          latency: 30000,
          jitter: 0,
        },
      }),
    });

    toxicInstalled = true;

    operation = await sendOperation({
      ingressUrl: integration.ingressUrl,
      integrationId: integration.id,
      key,
      payload: {
        id: key,
        eventId: key,
        type: 'checker.event',
        test: 'toxiproxy-network-failure',
      },
    });

    if (operation.statusCode !== 202) {
      throw new Error(
        `Webhook ingestion failed with HTTP ${operation.statusCode}.`,
      );
    }

    console.log('Fault installed. Waiting for a retry state.');

    const observedDelivery = await waitForRetry(integration.id);

    // Restore normal network conditions immediately after observing
    // the state change, or when the observation timeout expires.
    await removeToxic();
    toxicInstalled = false;

    if (!observedDelivery) {
      throw new Error(
        'No terminal or retry state was observed within the timeout.',
      );
    }

    console.log(
      `Observed delivery status: ${observedDelivery.status}; ` +
      `attempts: ${observedDelivery.attempt_count}`,
    );

    const audit = await waitForTerminalDeliveries({
      pool,
      integrationId: integration.id,
      expectedCount: 1,
      timeoutMs: 180000,
    });

    const finalAudit = await getAudit(pool, integration.id);

    const report = checkHistory(
      {
        ...finalAudit,
        operations: [operation],
      },
      {
        requireSuccess: true,
        expectedUniqueEvents: 1,
      },
    );

    report.metadata = {
      campaign: 'toxiproxy-network-failure',
      integrationId: integration.id,
      observedFaultState: observedDelivery.status,
      attemptsAtFaultObservation: observedDelivery.attempt_count,
      finalDeliveryCount: audit.deliveries.length,
    };

    const delivery = finalAudit.deliveries[0];

    if (observedDelivery.status !== 'retry_wait') {
      report.violations.push({
        property: 'expected_retry_state_not_observed',
        observedStatus: observedDelivery.status,
      });
    }

    if (Number(delivery?.attempt_count ?? 0) < 2) {
      report.violations.push({
        property: 'retry_attempt_not_observed',
        attemptCount: Number(delivery?.attempt_count ?? 0),
      });
    }

    report.passed = report.violations.length === 0;
    report.counts.violations = report.violations.length;

    console.log(JSON.stringify(report, null, 2));

    if (!report.passed) {
      process.exitCode = 1;
    }
  } finally {
    if (toxicInstalled) {
      await removeToxic();
    }

    try {
      await setSinkMode('success');
    } catch (error) {
      console.warn(`Could not reset sink: ${error.message}`);
    }
  }
}

try {
  await main();
} finally {
  await pool.end();
}