import pg from 'pg';

const { Pool } = pg;

export function createPool(config, logger) {
  const pool = new Pool({
    connectionString: config.databaseUrl,

    max: Number.parseInt(
      process.env.PG_POOL_MAX ?? '20',
      10
    ),

    idleTimeoutMillis: 30000,

    connectionTimeoutMillis: 5000,

    application_name: 'hookguard'
  });

  pool.on('error', (error) => {
    logger.error(
      { err: error },
      'Unexpected PostgreSQL idle-client error'
    );
  });

  return pool;
}