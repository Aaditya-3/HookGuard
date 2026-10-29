import { constantTimeEqual } from '../services/secrets.js';
import { AppError } from '../lib/errors.js';

export function adminAuth(config) {
  return (req, res, next) => {
    const provided = req.get('x-admin-token') ?? '';

    if (
      !constantTimeEqual(
        provided,
        config.adminApiToken
      )
    ) {
      return next(
        new AppError(
          401,
          'UNAUTHORIZED',
          'A valid X-Admin-Token header is required'
        )
      );
    }

    return next();
  };
}