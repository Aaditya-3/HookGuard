import 'dotenv/config';
import fc from 'fast-check';
import { mkdir, writeFile } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

import { checkHistory } from './history-checker.js';

import {
  createCheckerIntegration,
  createCheckerPool,
  getAudit,
  runConcurrent,
  sendOperation,
  waitForTerminalDeliveries,
} from './campaign-utils.js';

const here = path.dirname(fileURLToPath(import.meta.url));

const args = Object.fromEntries(
  process.argv.slice(2).map((argument) => {
    const [key, value = 'true'] = argument
      .replace(/^--/, '')
      .split('=');

    return [key, value];
  }),
);

const operationsCount = Number.parseInt(
  args.operations ?? process.env.CHECKER_OPERATIONS ?? '1000',
  10,
);

const concurrency = Number.parseInt(
  args.concurrency ?? process.env.CHECKER_CONCURRENCY ?? '50',
  10,
);

const seed = Number.parseInt(
  args.seed ?? process.env.CHECKER_SEED ?? '20261029',
  10,
);

const baseUrl = (
  process.env.HOOKGUARD_BASE_URL ?? 'http://localhost:3000'
).replace(/\/$/, '');

const adminToken = process.env.ADMIN_API_TOKEN;

const destinationUrl =
  process.env.CHECKER_DESTINATION_URL ??
  'http://webhook-sink:4001/webhook';

if (!adminToken) {
  throw new Error('Set ADMIN_API_TOKEN in your .env file.');
}

if (!Number.isInteger(operationsCount) || operationsCount < 1) {
  throw new Error('--operations must be a positive integer.');
}

if (!Number.isInteger(concurrency) || concurrency < 1) {
  throw new Error('--concurrency must be a positive integer.');
}

if (!Number.isInteger(seed)) {
  throw new Error('--seed must be an integer.');
}

const pool = createCheckerPool();

try {
  console.log(
    `Starting correctness campaign: ` +
    `operations=${operationsCount}, ` +
    `concurrency=${concurrency}, seed=${seed}`,
  );

  const created = await createCheckerIntegration({
    baseUrl,
    adminToken,
    destinationUrl,
  });

  const integration = created.integration;

  // Generate repeated logical keys. Every submission sharing a key
  // gets identical event content, which is essential for testing
  // legitimate idempotent retries.
  const keySpaceSize = Math.max(
    1,
    Math.floor(operationsCount / 2),
  );

  const generatedKeys = fc.sample(
    fc.integer({
      min: 0,
      max: keySpaceSize - 1,
    }),
    {
      numRuns: operationsCount,
      seed,
    },
  );

  const workload = generatedKeys.map((generatedKey) => {
    const key = `campaign-${generatedKey}`;

    return {
      key,
      payload: {
        id: key,
        eventId: key,
        type: 'checker.event',
        stableValue: generatedKey,
      },
    };
  });

  const expectedUniqueEvents = new Set(
    workload.map((operation) => operation.key),
  ).size;

  const campaignStartedAt = Date.now();

  const operations = await runConcurrent(
    workload,
    concurrency,
    (operation) =>
      sendOperation({
        ingressUrl: integration.ingressUrl,
        integrationId: integration.id,
        key: operation.key,
        payload: operation.payload,
      }),
  );

  const submissionTimeMs = Date.now() - campaignStartedAt;

  const rejectedOperations = operations.filter(
    (operation) => operation.statusCode !== 202,
  );

  console.log(
    `Submissions finished in ${(submissionTimeMs / 1000).toFixed(2)}s.`,
  );

  console.log(
    `Requests=${operations.length}, ` +
    `uniqueKeys=${expectedUniqueEvents}, ` +
    `non-202 responses=${rejectedOperations.length}`,
  );

  const audit = await waitForTerminalDeliveries({
    pool,
    integrationId: integration.id,
    expectedCount: expectedUniqueEvents,
    timeoutMs: 180000,
  });

  // Query once more after waiting so the final report contains the
  // latest database records observed by the campaign.
  const finalAudit = await getAudit(pool, integration.id);

  const report = checkHistory(
    {
      ...finalAudit,
      operations,
    },
    {
      requireSuccess: true,
      expectedUniqueEvents,
    },
  );

  report.metadata = {
    integrationId: integration.id,
    operationsRequested: operationsCount,
    concurrency,
    seed,
    expectedUniqueEvents,
    operationsAccepted: operations.filter(
      (operation) => operation.statusCode === 202,
    ).length,
    non202Responses: rejectedOperations.length,
    submissionTimeMs,
    finalDeliveryCount: audit.deliveries.length,
    elapsedMs: Date.now() - campaignStartedAt,
    note:
      'This is a finite test campaign, not a mathematical proof of correctness.',
  };

  report.rejectedSamples = rejectedOperations.slice(0, 20);

  const reportDirectory = path.join(here, '..', 'reports');

  await mkdir(reportDirectory, { recursive: true });

  const reportPath = path.join(
    reportDirectory,
    `correctness-${Date.now()}.json`,
  );

  await writeFile(
    reportPath,
    JSON.stringify(report, null, 2),
    'utf8',
  );

  console.log('\nCorrectness report:');
  console.log(JSON.stringify(report, null, 2));
  console.log(`\nReport written to ${reportPath}`);

  if (!report.passed || rejectedOperations.length > 0) {
    process.exitCode = 1;
  }
} finally {
  await pool.end();
}