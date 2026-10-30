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

const destinationUrl =
  process.env.CHECKER_DESTINATION_URL ??
  'http://webhook-sink:4001/webhook';

const pool = createCheckerPool();

async function runDockerCompose(args) {
  const result = await execFileAsync(
    'docker',
    ['compose', ...args],
    {
      cwd: projectRoot,
      timeout: 30000,
    },
  );

  if (result.stdout.trim()) {
    console.log(result.stdout.trim());
  }

  if (result.stderr.trim()) {
    console.error(result.stderr.trim());
  }
}

async function setSinkMode(mode, delayMs = 0) {
  const response = await fetch(
    'http://127.0.0.1:4001/__control',
    {
      method: 'POST',
      headers: {
        'content-type': 'application/json',
      },
      body: JSON.stringify({
        mode,
        delayMs,
      }),
      signal: AbortSignal.timeout(5000),
    },
  );

  if (!response.ok) {
    throw new Error(
      `Could not set webhook sink mode: HTTP ${response.status}`,
    );
  }
}

async function waitForProcessing(integrationId, timeoutMs = 15000) {
  const deadline = Date.now() + timeoutMs;

  while (Date.now() < deadline) {
    const result = await pool.query(
      `SELECT id, status, lease_expires_at
       FROM deliveries
       WHERE integration_id = $1
       ORDER BY created_at DESC
       LIMIT 1`,
      [integrationId],
    );

    const delivery = result.rows[0];

    if (delivery?.status === 'processing') {
      return delivery;
    }

    await new Promise((resolve) => setTimeout(resolve, 200));
  }

  throw new Error(
    'Timed out waiting for the delivery to enter processing state.',
  );
}

async function main() {
  if (!adminToken) {
    throw new Error('ADMIN_API_TOKEN is missing from .env.');
  }

  console.log('Starting worker-crash recovery campaign.');

  const liveResponse = await fetch(`${baseUrl}/health/live`);

  if (!liveResponse.ok) {
    throw new Error('HookGuard API is not healthy.');
  }

  const created = await createCheckerIntegration({
    baseUrl,
    adminToken,
    destinationUrl,
  });

  const integration = created.integration;
  const key = `worker-crash-${Date.now()}`;

  let operation;

  try {
    // Hold the outgoing request open so we can crash the worker
    // while the delivery is being processed.
    await setSinkMode('delay', 20000);

    operation = await sendOperation({
      ingressUrl: integration.ingressUrl,
      integrationId: integration.id,
      key,
      payload: {
        id: key,
        eventId: key,
        type: 'checker.event',
        test: 'worker-crash-recovery',
      },
    });

    if (operation.statusCode !== 202) {
      throw new Error(
        `Webhook ingestion failed with HTTP ${operation.statusCode}.`,
      );
    }

    const delivery = await waitForProcessing(integration.id);

    console.log(
      `Delivery ${delivery.id} is processing; killing worker.`,
    );

    await runDockerCompose([
      'kill',
      '--signal=SIGKILL',
      'worker',
    ]);

    console.log('Worker killed.');

    // Allow the container to stop before restarting the worker.
    await new Promise((resolve) => setTimeout(resolve, 1000));

    await runDockerCompose(['up', '-d', 'worker']);

    console.log('Worker restarted.');

    // Future delivery attempts should now receive a normal response.
    await setSinkMode('success');

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
      campaign: 'worker-crash-recovery',
      integrationId: integration.id,
      deliveryId: delivery.id,
      finalDeliveryCount: audit.deliveries.length,
      note:
        'One controlled worker crash was attempted. ' +
        'This is not a large-scale crash campaign.',
    };

    console.log(JSON.stringify(report, null, 2));

    if (!report.passed) {
      process.exitCode = 1;
    }
  } finally {
    // Restore the sink and ensure the worker is running even if
    // an assertion or database query fails partway through.
    try {
      await setSinkMode('success');
    } catch (error) {
      console.error(`Warning: could not reset sink mode: ${error.message}`);
    }

    try {
      await runDockerCompose(['up', '-d', 'worker']);
    } catch (error) {
      console.error(`Warning: could not restart worker: ${error.message}`);
    }
  }
}

try {
  await main();
} finally {
  await pool.end();
}