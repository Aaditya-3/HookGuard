import { randomUUID } from 'node:crypto';

import {
  createOpaqueToken,
  encryptSecret,
  hashToken
} from './secrets.js';

import {
  validateDestinationUrl
} from './destination-validation.js';

export async function createIntegration({
  pool,
  config,
  input
}) {
  const destinationUrl = await validateDestinationUrl(
    input.destinationUrl,
    {
      allowPrivate: config.allowPrivateDestinations,
      requireHttps: config.nodeEnv === 'production'
    }
  );

  const id = randomUUID();

  const ingressToken = createOpaqueToken();

  const destinationSigningSecret = createOpaqueToken();

  const ingressTokenHash = hashToken(ingressToken);

  const encryptedSigningSecret = encryptSecret(
    destinationSigningSecret,
    config.encryptionKey
  );

  const result = await pool.query(
    `
      INSERT INTO integrations (
        id,
        name,
        source_service,
        subscribed_event_types,
        destination_url,
        ingress_token_hash,
        destination_signing_secret_enc,
        status,
        created_at,
        updated_at
      )
      VALUES (
        $1, $2, $3, $4, $5, $6, $7,
        'active', NOW(), NOW()
      )
      RETURNING
        id,
        name,
        source_service,
        subscribed_event_types,
        destination_url,
        status,
        created_at
    `,
    [
      id,
      input.name,
      input.sourceService,
      input.eventTypes,
      destinationUrl,
      ingressTokenHash,
      encryptedSigningSecret
    ]
  );

  const row = result.rows[0];

  return {
    integration: {
      id: row.id,
      name: row.name,
      sourceService: row.source_service,
      eventTypes: row.subscribed_event_types,
      destinationUrl: row.destination_url,
      status: row.status,
      createdAt: row.created_at
    },

    // Show the ingress URL only when it is created.
    ingressUrl:
      `${config.ingressBaseUrl}/ingest/${ingressToken}`,

    // Display this to the user once so their receiver can
    // verify outgoing webhook signatures.
    destinationSigningSecret
  };
}

export async function listIntegrations({ pool }) {
  const result = await pool.query(
    `
      SELECT
        id,
        name,
        source_service,
        subscribed_event_types,
        destination_url,
        status,
        created_at,
        updated_at
      FROM integrations
      ORDER BY created_at DESC
      LIMIT 200
    `
  );

  return result.rows.map((row) => ({
    id: row.id,
    name: row.name,
    sourceService: row.source_service,
    eventTypes: row.subscribed_event_types,
    destinationUrl: row.destination_url,
    status: row.status,
    createdAt: row.created_at,
    updatedAt: row.updated_at
  }));
}

export async function setIntegrationStatus({
  pool,
  integrationId,
  status
}) {
  const result = await pool.query(
    `
      UPDATE integrations
      SET
        status = $2,
        updated_at = NOW()
      WHERE id = $1
      RETURNING
        id,
        name,
        source_service,
        subscribed_event_types,
        destination_url,
        status,
        created_at,
        updated_at
    `,
    [integrationId, status]
  );

  if (result.rowCount === 0) {
    return null;
  }

  const row = result.rows[0];

  return {
    id: row.id,
    name: row.name,
    sourceService: row.source_service,
    eventTypes: row.subscribed_event_types,
    destinationUrl: row.destination_url,
    status: row.status,
    createdAt: row.created_at,
    updatedAt: row.updated_at
  };
}