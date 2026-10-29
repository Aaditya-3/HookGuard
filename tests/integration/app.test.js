import {
  afterEach,
  describe,
  expect,
  it,
  beforeEach,
  vi
} from 'vitest';

import request from 'supertest';

import { createApp } from '../../src/app.js';
import { createMetrics } from '../../src/metrics.js';

describe('HookGuard API', () => {
  let app;
  let pool;
  let redis;
  let logger;

  beforeEach(() => {
    pool = {
      query: vi.fn().mockResolvedValue({
        rows: [],
        rowCount: 0
      }),

      connect: vi.fn()
    };

    redis = {
      ping: vi.fn().mockResolvedValue('PONG')
    };

    logger = {
      info: vi.fn(),
      warn: vi.fn(),
      error: vi.fn()
    };

    const config = {
      nodeEnv: 'test',
      adminApiToken: 'test-admin-token-123456789',
      ingressBaseUrl: 'http://localhost:3000',
      allowPrivateDestinations: true
    };

    app = createApp({
      pool,
      redis,
      config,
      logger,
      metrics: createMetrics('test')
    });
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  it('returns a successful liveness response', async () => {
    const response = await request(app)
      .get('/health/live');

    expect(response.status).toBe(200);

    expect(response.body).toEqual({
      status: 'ok'
    });
  });

  it('returns 404 for an unknown route', async () => {
    const response = await request(app)
      .get('/route-that-does-not-exist');

    expect(response.status).toBe(404);

    expect(response.body.error.code).toBe(
      'NOT_FOUND'
    );
  });

  it('returns 400 for malformed JSON', async () => {
    const response = await request(app)
      .post('/unknown')
      .set('Content-Type', 'application/json')
      .send('{"event":');

    expect(response.status).toBe(400);

    expect(response.body.error.code).toBe(
      'INVALID_JSON'
    );
  });

  it('returns readiness when PostgreSQL and Redis respond', async () => {
    const response = await request(app)
      .get('/health/ready');

    expect(response.status).toBe(200);

    expect(response.body.status).toBe('ready');

    expect(pool.query).toHaveBeenCalledWith(
      'SELECT 1'
    );

    expect(redis.ping).toHaveBeenCalled();
  });

  it('returns 503 when a dependency is unavailable', async () => {
    redis.ping.mockRejectedValueOnce(
      new Error('Redis unavailable')
    );

    const response = await request(app)
      .get('/health/ready');

    expect(response.status).toBe(503);

    expect(response.body.error.code).toBe(
      'NOT_READY'
    );
  });

  it('returns metrics in Prometheus format', async () => {
    const response = await request(app)
      .get('/metrics');

    expect(response.status).toBe(200);

    expect(response.text).toContain(
      'hookguard_ingress_requests_total'
    );
  });
});