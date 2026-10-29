import express from 'express';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { randomUUID } from 'node:crypto';

import { createIntegrationsRouter } from './routes/integrations.js';
import { createIngestionRouter } from './routes/ingest.js';
import { createDeliveriesRouter } from './routes/deliveries.js';

import { adminAuth } from './middleware/admin-auth.js';
import { AppError } from './lib/errors.js';
import { createMetrics } from './metrics.js';

const here = path.dirname(
  fileURLToPath(import.meta.url)
);

export function createApp({
  pool,
  redis,
  config,
  logger,
  metrics = createMetrics('api')
}) {
  const app = express();

  app.disable('x-powered-by');

  // Assign a request ID and log request completion.
  app.use((req, res, next) => {
    const requestId =
      req.get('x-request-id')?.slice(0, 128) ||
      randomUUID();

    req.requestId = requestId;

    res.set('X-Request-Id', requestId);

    const startedAt = process.hrtime.bigint();

    res.on('finish', () => {
      logger.info(
        {
          requestId,
          method: req.method,
          statusCode: res.statusCode,
          durationMs:
            Number(
              process.hrtime.bigint() - startedAt
            ) / 1e6
        },
        'HTTP request completed'
      );
    });

    next();
  });

  // Parse JSON and preserve the raw bytes for HMAC verification.
  app.use(
    express.json({
      limit: '256kb',

      verify(req, res, buffer) {
        req.rawBody = Buffer.from(buffer);
      }
    })
  );

  // Liveness check.
  app.get('/health/live', (req, res) => {
    res.status(200).json({
      status: 'ok'
    });
  });

  // Readiness check for required infrastructure.
  app.get('/health/ready', async (req, res, next) => {
    try {
      await pool.query('SELECT 1');

      await redis.ping();

      res.status(200).json({
        status: 'ready',
        dependencies: {
          postgres: 'ok',
          redis: 'ok'
        }
      });
    } catch {
      next(
        new AppError(
          503,
          'NOT_READY',
          'A required dependency is not ready'
        )
      );
    }
  });

  // Prometheus metrics endpoint.
  app.get('/metrics', async (req, res, next) => {
    try {
      res.set(
        'Content-Type',
        metrics.registry.contentType
      );

      res.end(
        await metrics.registry.metrics()
      );
    } catch (error) {
      next(error);
    }
  });

  // Protect administrative API routes.
  app.use('/api', adminAuth(config));

  app.use(
    '/api/integrations',
    createIntegrationsRouter({
      pool,
      config,
      logger
    })
  );

  app.use(
    '/api/deliveries',
    createDeliveriesRouter({
      pool,
      logger
    })
  );

  // Public webhook ingress uses its unique token.
  app.use(
    '/ingest',
    createIngestionRouter({
      pool,
      redis,
      config,
      metrics,
      logger
    })
  );

  // Serve the dashboard.
  app.use(
    express.static(
      path.join(here, '..', 'public'),
      {
        index: 'index.html',
        fallthrough: true
      }
    )
  );

  // Unknown routes.
  app.use((req, res) => {
    res.status(404).json({
      error: {
        code: 'NOT_FOUND',
        message: 'Route not found'
      }
    });
  });

  // Centralized error handling.
  app.use((err, req, res, next) => {
    if (res.headersSent) {
      return next(err);
    }

    if (err?.type === 'entity.parse.failed') {
      return res.status(400).json({
        error: {
          code: 'INVALID_JSON',
          message: 'Request body must contain valid JSON'
        }
      });
    }

    if (err?.type === 'entity.too.large') {
      return res.status(413).json({
        error: {
          code: 'PAYLOAD_TOO_LARGE',
          message: 'Request body exceeds the 256 KB limit'
        }
      });
    }

    if (err instanceof AppError) {
      return res.status(err.statusCode).json({
        error: {
          code: err.code,
          message: err.message,
          ...(err.details
            ? { details: err.details }
            : {})
        }
      });
    }

    logger.error(
      {
        err,
        requestId: req.requestId
      },
      'Unhandled request error'
    );

    return res.status(500).json({
      error: {
        code: 'INTERNAL_ERROR',
        message: 'An unexpected error occurred',
        requestId: req.requestId
      }
    });
  });

  return app;
}