import 'dotenv/config';

import pg from 'pg';

import {
  readdir,
  readFile
} from 'node:fs/promises';

import path from 'node:path';

import {
  fileURLToPath
} from 'node:url';

const { Pool } = pg;

const here = path.dirname(
  fileURLToPath(import.meta.url)
);

const migrationsDirectory = path.join(
  here,
  '..',
  'src',
  'db',
  'migrations'
);

const databaseUrl = process.env.DATABASE_URL;

if (!databaseUrl) {
  throw new Error(
    'DATABASE_URL is required. Check your .env file.'
  );
}

const pool = new Pool({
  connectionString: databaseUrl,
  max: 2,
  connectionTimeoutMillis: 5000
});

async function runMigrations() {
  const client = await pool.connect();

  try {
    await client.query('BEGIN');

    // Prevent two migration processes from running
    // the migration sequence concurrently.
    await client.query(
      "SELECT pg_advisory_xact_lock(hashtext('hookguard:migrations'))"
    );

    await client.query(`
      CREATE TABLE IF NOT EXISTS schema_migrations (
        name TEXT PRIMARY KEY,
        applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
      )
    `);

    await client.query('COMMIT');

    const files = (
      await readdir(migrationsDirectory)
    )
      .filter((file) => /^\d+.*\.sql$/.test(file))
      .sort();

    for (const file of files) {
      await client.query('BEGIN');

      try {
        await client.query(
          "SELECT pg_advisory_xact_lock(hashtext('hookguard:migrations'))"
        );

        const existing = await client.query(
          `
            SELECT name
            FROM schema_migrations
            WHERE name = $1
          `,
          [file]
        );

        if (existing.rowCount > 0) {
          console.log(`Already applied: ${file}`);

          await client.query('COMMIT');

          continue;
        }

        const sql = await readFile(
          path.join(migrationsDirectory, file),
          'utf8'
        );

        console.log(`Applying migration: ${file}`);

        await client.query(sql);

        await client.query(
          `
            INSERT INTO schema_migrations (name)
            VALUES ($1)
          `,
          [file]
        );

        await client.query('COMMIT');

        console.log(`Applied successfully: ${file}`);
      } catch (error) {
        await client.query('ROLLBACK').catch(() => {});

        throw error;
      }
    }

    console.log('Database migrations completed.');
  } finally {
    client.release();
  }
}

try {
  await runMigrations();
} catch (error) {
  console.error('Database migration failed:', error.message);

  process.exitCode = 1;
} finally {
  await pool.end();
}