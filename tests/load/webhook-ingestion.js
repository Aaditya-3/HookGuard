import http from 'k6/http';
import { check } from 'k6';
import { Counter, Rate } from 'k6/metrics';

const acceptedRequests = new Counter(
  'hookguard_accepted_requests',
);

const acceptanceRate = new Rate(
  'hookguard_ingestion_acceptance_rate',
);

const ingressUrl = __ENV.HOOKGUARD_INGRESS_URL;
const runId = __ENV.RUN_ID || 'local';

if (!ingressUrl) {
  throw new Error(
    'Set HOOKGUARD_INGRESS_URL to the generated integration ingress URL.',
  );
}

const targetRate = Number(__ENV.RATE || 20);
const duration = __ENV.DURATION || '1m';
const maxVUs = Number(__ENV.MAX_VUS || 100);
const preAllocatedVUs = Number(__ENV.PRE_ALLOCATED_VUS || 20);
const p95Limit = Number(__ENV.P95_MS || 1500);

if (
  !Number.isInteger(targetRate) ||
  targetRate < 1 ||
  !Number.isInteger(maxVUs) ||
  !Number.isInteger(preAllocatedVUs) ||
  maxVUs < preAllocatedVUs ||
  preAllocatedVUs < 1
) {
  throw new Error('Invalid k6 load-test configuration.');
}

export const options = {
  scenarios: {
    webhook_ingestion: {
      executor: 'constant-arrival-rate',
      rate: targetRate,
      timeUnit: '1s',
      duration,
      preAllocatedVUs,
      maxVUs,
      gracefulStop: '15s',
    },
  },

  thresholds: {
    http_req_failed: ['rate<0.01'],
    http_req_duration: [`p(95)<${p95Limit}`],
    hookguard_ingestion_acceptance_rate: ['rate>0.99'],
    http_reqs: ['count>=1000'],
  },
};

export default function () {
  // __VU identifies the virtual user.
  // __ITER identifies that user's current iteration.
  // Their combination produces a unique logical event ID.
 const sourceEventId = `k6-${runId}-${__VU}-${__ITER}`;

  const payload = {
    id: sourceEventId,
    eventId: sourceEventId,
    type: 'checker.event',
    source: 'hookguard-k6',
    timestamp: new Date().toISOString(),
    data: {
      customerId: `customer-${__VU}`,
      value: __ITER,
    },
  };

  const response = http.post(
    ingressUrl,
    JSON.stringify(payload),
    {
      headers: {
        'content-type': 'application/json',
        'x-event-type': 'checker.event',
        'x-event-id': sourceEventId,
        'x-source-event-id': sourceEventId,
        'idempotency-key': sourceEventId,
        'x-idempotency-key': sourceEventId,
      },
      timeout: '15s',
      tags: {
        endpoint: 'webhook_ingestion',
      },
    },
  );

  const accepted = check(response, {
    'webhook accepted with HTTP 202': (res) => res.status === 202,
  });

  acceptanceRate.add(accepted);

  if (accepted) {
    acceptedRequests.add(1);
  }
}

export function handleSummary(data) {
  const summary = {
    totalRequests: data.metrics.http_reqs?.values?.count ?? 0,

    acceptedRequests:
      data.metrics.hookguard_accepted_requests?.values?.count ?? 0,

    acceptanceRate:
      data.metrics.hookguard_ingestion_acceptance_rate?.values?.rate ?? 0,

    averageLatency:
      data.metrics.http_req_duration?.values?.avg ?? 0,

    p95Latency:
      data.metrics.http_req_duration?.values?.['p(95)'] ?? 0,

    failedRequestRate:
      data.metrics.http_req_failed?.values?.rate ?? 0,
  };

  return {
    stdout: JSON.stringify(summary, null, 2),
    'reports/k6-summary.json': JSON.stringify(data, null, 2),
  };
}