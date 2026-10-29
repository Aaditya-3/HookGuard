import {
  describe,
  expect,
  it
} from 'vitest';

import {
  signPayload,
  verifyPayloadSignature
} from '../../src/lib/signatures.js';

describe('webhook signatures', () => {
  const secret = 'local-test-signing-secret';
  const timestamp = 1793282400;

  const body = JSON.stringify({
    id: 'evt_123',
    type: 'payment.succeeded',
    data: {
      paymentId: 'pay_123'
    }
  });

  it('verifies a valid signature', () => {
    const signature = signPayload(
      secret,
      String(timestamp),
      body
    );

    expect(
      verifyPayloadSignature({
        secret,
        timestamp,
        body,
        signature,
        nowSeconds: timestamp
      })
    ).toBe(true);
  });

  it('rejects a modified body', () => {
    const signature = signPayload(
      secret,
      String(timestamp),
      body
    );

    expect(
      verifyPayloadSignature({
        secret,
        timestamp,
        body: `${body} `,
        signature,
        nowSeconds: timestamp
      })
    ).toBe(false);
  });

  it('rejects an incorrect secret', () => {
    const signature = signPayload(
      secret,
      String(timestamp),
      body
    );

    expect(
      verifyPayloadSignature({
        secret: 'wrong-secret',
        timestamp,
        body,
        signature,
        nowSeconds: timestamp
      })
    ).toBe(false);
  });

  it('rejects an old timestamp', () => {
    const signature = signPayload(
      secret,
      String(timestamp),
      body
    );

    expect(
      verifyPayloadSignature({
        secret,
        timestamp,
        body,
        signature,
        nowSeconds: timestamp + 1000
      })
    ).toBe(false);
  });

  it('rejects malformed signatures', () => {
    expect(
      verifyPayloadSignature({
        secret,
        timestamp,
        body,
        signature: 'not-a-signature',
        nowSeconds: timestamp
      })
    ).toBe(false);
  });
});