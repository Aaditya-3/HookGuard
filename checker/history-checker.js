import { isAllowedTransition } from '../src/lib/state-machine.js';

const TERMINAL_STATES = new Set(['succeeded', 'dead_letter']);

export function checkDuplicateLogicalEvents(events, operations = []) {
  const violations = [];
  const seenEvents = new Map();

  // Check whether PostgreSQL contains multiple event records for the
  // same source event within the same integration.
  for (const event of events) {
    if (!event.source_event_id) {
      continue;
    }

    const key = `${event.integration_id}:${event.source_event_id}`;
    const previousId = seenEvents.get(key);

    if (previousId && previousId !== event.id) {
      violations.push({
        property: 'duplicate_logical_event',
        key,
        eventIds: [previousId, event.id],
      });
    } else {
      seenEvents.set(key, event.id);
    }
  }

  // Verify that repeated submissions of the same logical operation
  // do not return different event IDs.
  const responseEvents = new Map();

  for (const operation of operations) {
    if (!operation.idempotencyKey || !operation.eventId) {
      continue;
    }

    const key = `${operation.integrationId}:${operation.idempotencyKey}`;
    const previousId = responseEvents.get(key);

    if (previousId && previousId !== operation.eventId) {
      violations.push({
        property: 'duplicate_event_response',
        key,
        eventIds: [previousId, operation.eventId],
      });
    } else {
      responseEvents.set(key, operation.eventId);
    }
  }

  return violations;
}

export function checkStateTransitions(transitions) {
  const violations = [];
  const grouped = new Map();

  const ordered = [...transitions].sort(
    (a, b) => Number(a.id) - Number(b.id),
  );

  // Group transition records by delivery.
  for (const transition of ordered) {
    if (!grouped.has(transition.delivery_id)) {
      grouped.set(transition.delivery_id, []);
    }

    grouped.get(transition.delivery_id).push(transition);
  }

  // Check each delivery's transition history.
  for (const [deliveryId, chain] of grouped) {
    let previousState;

    for (const transition of chain) {
      const from = transition.from_status ?? null;
      const to = transition.to_status;

      if (!isAllowedTransition(from, to)) {
        violations.push({
          property: 'invalid_state_transition',
          deliveryId,
          transitionId: transition.id,
          from,
          to,
        });
      }

      if (
        previousState !== undefined &&
        from !== previousState
      ) {
        violations.push({
          property: 'broken_state_history',
          deliveryId,
          transitionId: transition.id,
          expectedFrom: previousState,
          actualFrom: from,
        });
      }

      previousState = to;
    }
  }

  return violations;
}

export function checkIncompleteDeliveries(deliveries) {
  return deliveries
    .filter((delivery) => !TERMINAL_STATES.has(delivery.status))
    .map((delivery) => ({
      property: 'incomplete_delivery',
      deliveryId: delivery.id,
      status: delivery.status,
    }));
}

export function checkEventDeliveryRelationship(events, deliveries) {
  const violations = [];
  const deliveryCounts = new Map();

  for (const delivery of deliveries) {
    deliveryCounts.set(
      delivery.event_id,
      (deliveryCounts.get(delivery.event_id) ?? 0) + 1,
    );
  }

  for (const event of events) {
    const count = deliveryCounts.get(event.id) ?? 0;

    if (count === 0) {
      violations.push({
        property: 'missing_delivery',
        eventId: event.id,
      });
    } else if (count > 1) {
      violations.push({
        property: 'multiple_deliveries_for_event',
        eventId: event.id,
        deliveryCount: count,
      });
    }
  }

  return violations;
}

export function checkAttemptConsistency(deliveries, attempts) {
  const violations = [];
  const attemptsByDelivery = new Map();

  for (const attempt of attempts) {
    if (!attempt.delivery_id) {
      continue;
    }

    if (!attemptsByDelivery.has(attempt.delivery_id)) {
      attemptsByDelivery.set(attempt.delivery_id, []);
    }

    attemptsByDelivery.get(attempt.delivery_id).push(attempt);
  }

  for (const delivery of deliveries) {
    const attemptsForDelivery =
      attemptsByDelivery.get(delivery.id) ?? [];

    // Only compare when the delivery record has an attempt counter.
    if (
      Number.isInteger(Number(delivery.attempt_count)) &&
      Number(delivery.attempt_count) !== attemptsForDelivery.length
    ) {
      violations.push({
        property: 'attempt_count_mismatch',
        deliveryId: delivery.id,
        recordedAttemptCount: Number(delivery.attempt_count),
        actualAttemptRecords: attemptsForDelivery.length,
      });
    }

    const numberedAttempts = attemptsForDelivery
      .filter((attempt) => attempt.attempt_number != null)
      .map((attempt) => Number(attempt.attempt_number))
      .sort((a, b) => a - b);

    // If attempt numbers are available, check that they form
    // a continuous sequence starting at 1.
    if (numberedAttempts.length > 0) {
      for (let index = 0; index < numberedAttempts.length; index += 1) {
        const expected = index + 1;

        if (numberedAttempts[index] !== expected) {
          violations.push({
            property: 'attempt_sequence_gap',
            deliveryId: delivery.id,
            expectedAttempt: expected,
            actualAttempt: numberedAttempts[index],
          });
        }
      }
    }
  }

  return violations;
}

export function checkHistory(
  history,
  {
    requireSuccess = true,
    expectedUniqueEvents,
  } = {},
) {
  const events = history.events ?? [];
  const deliveries = history.deliveries ?? [];
  const attempts = history.attempts ?? [];
  const transitions = history.transitions ?? [];
  const operations = history.operations ?? [];

  const violations = [
    ...checkDuplicateLogicalEvents(events, operations),
    ...checkIncompleteDeliveries(deliveries),
    ...checkEventDeliveryRelationship(events, deliveries),
    ...checkAttemptConsistency(deliveries, attempts),
  ];

  // Transition checks work when a durable transition history
  // has been collected for the campaign.
  if (transitions.length > 0) {
    violations.push(...checkStateTransitions(transitions));

    const finalStates = new Map();

    for (
      const transition of [...transitions].sort(
        (a, b) => Number(a.id) - Number(b.id),
      )
    ) {
      finalStates.set(
        transition.delivery_id,
        transition.to_status,
      );
    }

    for (const delivery of deliveries) {
      const lastRecordedState = finalStates.get(delivery.id);

      if (
        lastRecordedState !== undefined &&
        lastRecordedState !== delivery.status
      ) {
        violations.push({
          property: 'persisted_state_mismatch',
          deliveryId: delivery.id,
          persistedState: delivery.status,
          lastRecordedState,
        });
      }
    }
  }

  if (
    expectedUniqueEvents !== undefined &&
    events.length !== expectedUniqueEvents
  ) {
    violations.push({
      property: 'unique_event_count_mismatch',
      expected: expectedUniqueEvents,
      actual: events.length,
    });
  }

  const terminalFailures = deliveries.filter(
    (delivery) => delivery.status === 'dead_letter',
  );

  if (requireSuccess) {
    for (const delivery of terminalFailures) {
      violations.push({
        property: 'delivery_did_not_succeed',
        deliveryId: delivery.id,
        status: delivery.status,
      });
    }
  }

  return {
    passed: violations.length === 0,
    counts: {
      operations: operations.length,
      events: events.length,
      deliveries: deliveries.length,
      attempts: attempts.length,
      transitions: transitions.length,
      terminalFailures: terminalFailures.length,
      violations: violations.length,
    },
    violations,
  };
}