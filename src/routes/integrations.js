import { Router } from 'express';
import { z } from 'zod';

import { AppError } from '../lib/errors.js';

import {
  createIntegration,
  listIntegrations,
  setIntegrationStatus
} from '../services/integrations-service.js';

const createIntegrationSchema = z.object({
  name: z.string().trim().min(1).max(100),

  sourceService: z.string()
    .trim()
    .min(1)
    .max(100),

  eventTypes: z.array(
    z.string()
      .trim()
      .min(1)
      .max(200)
  ).min(1).max(100),

  destinationUrl: z.string().url()
}).strict();

const statusSchema = z.object({
  status: z.enum(['active', 'disabled'])
}).strict();

export function createIntegrationsRouter({
  pool,
  config,
  logger
}) {
  const router = Router();

  router.post('/', async (req, res, next) => {
    try {
      const parsed = createIntegrationSchema.safeParse(
        req.body
      );

      if (!parsed.success) {
        return next(
          new AppError(
            400,
            'INVALID_INTEGRATION',
            'Integration configuration is invalid',
            parsed.error.flatten()
          )
        );
      }

      const eventTypes = [
        ...new Set(parsed.data.eventTypes)
      ];

      const result = await createIntegration({
        pool,
        config,
        input: {
          ...parsed.data,
          eventTypes
        }
      });

      logger.info(
        {
          integrationId: result.integration.id
        },
        'Integration created'
      );

      return res.status(201).json(result);
    } catch (error) {
      return next(error);
    }
  });

  router.get('/', async (req, res, next) => {
    try {
      const integrations = await listIntegrations({
        pool
      });

      return res.status(200).json({
        integrations
      });
    } catch (error) {
      return next(error);
    }
  });

  router.patch('/:id/status', async (req, res, next) => {
    try {
      const parsedId = z.string().uuid().safeParse(
        req.params.id
      );

      const parsedBody = statusSchema.safeParse(
        req.body
      );

      if (!parsedId.success || !parsedBody.success) {
        return next(
          new AppError(
            400,
            'INVALID_STATUS_UPDATE',
            'Integration ID or status is invalid'
          )
        );
      }

      const integration = await setIntegrationStatus({
        pool,
        integrationId: parsedId.data,
        status: parsedBody.data.status
      });

      if (!integration) {
        return next(
          new AppError(
            404,
            'INTEGRATION_NOT_FOUND',
            'Integration not found'
          )
        );
      }

      return res.status(200).json({
        integration
      });
    } catch (error) {
      return next(error);
    }
  });

  return router;
}