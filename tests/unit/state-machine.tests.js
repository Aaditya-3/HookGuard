import {
  describe,
  expect,
  it
} from 'vitest';

import {
  DELIVERY_STATES,
  isAllowedTransition,
  assertAllowedTransition
} from '../../src/lib/state-machine.js';

describe('delivery state machine', () => {
  it('defines the expected states', () => {
    expect(DELIVERY_STATES).toEqual([
      'queued',
      'processing',
      'retry_wait',
      'succeeded',
      'dead_letter'
    ]);
  });

  it('allows a new delivery to be queued', () => {
    expect(
      isAllowedTransition(null, 'queued')
    ).toBe(true);
  });

  it('allows a queued delivery to start processing', () => {
    expect(
      isAllowedTransition('queued', 'processing')
    ).toBe(true);
  });

  it('allows a processing delivery to succeed', () => {
    expect(
      isAllowedTransition('processing', 'succeeded')
    ).toBe(true);
  });

  it('allows retries after an unsuccessful attempt', () => {
    expect(
      isAllowedTransition('processing', 'retry_wait')
    ).toBe(true);
  });

  it('allows a terminal delivery to be replayed', () => {
    expect(
      isAllowedTransition('dead_letter', 'queued')
    ).toBe(true);
  });

  it('rejects a successful delivery returning to processing', () => {
    expect(
      isAllowedTransition('succeeded', 'processing')
    ).toBe(false);
  });

  it('throws when an invalid transition is asserted', () => {
    expect(() => {
      assertAllowedTransition(
        'succeeded',
        'processing'
      );
    }).toThrow(
      'Invalid delivery state transition: succeeded -> processing'
    );
  });
});