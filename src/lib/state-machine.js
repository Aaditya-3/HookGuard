export const DELIVERY_STATES = Object.freeze([
  'queued',
  'processing',
  'retry_wait',
  'succeeded',
  'dead_letter'
]);

const transitions = new Set([
  'null->queued',

  'queued->processing',

  'processing->succeeded',
  'processing->retry_wait',
  'processing->dead_letter',

  // Recovery after an expired worker lease.
  'processing->queued',

  'retry_wait->queued',

  // Explicit manual replay.
  'dead_letter->queued'
]);

export function isAllowedTransition(
  fromStatus,
  toStatus
) {
  return transitions.has(
    `${fromStatus ?? 'null'}->${toStatus}`
  );
}

export function assertAllowedTransition(
  fromStatus,
  toStatus
) {
  if (!isAllowedTransition(fromStatus, toStatus)) {
    throw new Error(
      `Invalid delivery state transition: ${
        fromStatus ?? 'null'
      } -> ${toStatus}`
    );
  }
}