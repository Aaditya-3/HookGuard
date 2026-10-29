import { describe, expect, it } from 'vitest';

import {
  checkDuplicateLogicalEvents,
  checkEventDeliveryRelationship,
  checkIncompleteDeliveries,
  checkStateTransitions,
  checkHistory,
} from '../../checker/history-checker.js';

describe('HookGuard correctness checker', () => {
  it('detects duplicate logical events', () => {
    const events = [
      {
        id: 'event-1',
        integration_id: 'integration-1',
        source_event_id: 'source-1',
      },
      {
        id: 'event-2',
        integration_id: 'integration-1',
        source_event_id: 'source-1',
      },
    ];

    const violations = checkDuplicateLogicalEvents(events);

    expect(violations).toHaveLength(1);
    expect(violations[0].property).toBe('duplicate_logical_event');
  });

  it('does not flag different events as duplicates', () => {
    const events = [
      {
        id: 'event-1',
        integration_id: 'integration-1',
        source_event_id: 'source-1',
      },
      {
        id: 'event-2',
        integration_id: 'integration-1',
        source_event_id: 'source-2',
      },
    ];

    expect(checkDuplicateLogicalEvents(events)).toHaveLength(0);
  });

  it('detects events without a delivery', () => {
    const events = [
      {
        id: 'event-1',
        integration_id: 'integration-1',
      },
    ];

    const violations = checkEventDeliveryRelationship(events, []);

    expect(violations).toHaveLength(1);
    expect(violations[0].property).toBe('missing_delivery');
  });

  it('detects deliveries that are not terminal', () => {
    const deliveries = [
      {
        id: 'delivery-1',
        status: 'processing',
      },
      {
        id: 'delivery-2',
        status: 'succeeded',
      },
    ];

    const violations = checkIncompleteDeliveries(deliveries);

    expect(violations).toHaveLength(1);
    expect(violations[0].deliveryId).toBe('delivery-1');
  });

  it('detects invalid state transitions', () => {
    const transitions = [
      {
        id: 1,
        delivery_id: 'delivery-1',
        from_status: null,
        to_status: 'queued',
      },
      {
        id: 2,
        delivery_id: 'delivery-1',
        from_status: 'queued',
        to_status: 'succeeded',
      },
    ];

    const violations = checkStateTransitions(transitions);

    // Use the project's state-machine rules as the authority.
    // This assertion expects queued -> succeeded to be rejected
    // by the current state machine.
    expect(violations.length).toBeGreaterThan(0);
  });

  it('passes a clean history with one successful delivery', () => {
    const history = {
      events: [
        {
          id: 'event-1',
          integration_id: 'integration-1',
          source_event_id: 'source-1',
        },
      ],
      deliveries: [
        {
          id: 'delivery-1',
          event_id: 'event-1',
          integration_id: 'integration-1',
          status: 'succeeded',
          attempt_count: 0,
        },
      ],
      attempts: [],
      operations: [],
      transitions: [],
    };

    const report = checkHistory(history, {
      requireSuccess: true,
      expectedUniqueEvents: 1,
    });

    expect(report.passed).toBe(true);
    expect(report.counts.violations).toBe(0);
  });
});