import {
  describe,
  expect,
  it
} from 'vitest';

import {
  createOpaqueToken,
  hashToken,
  constantTimeEqual,
  encryptSecret,
  decryptSecret
} from '../../src/services/secrets.js';

const key = Buffer.from(
  '0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef',
  'hex'
);

describe('secret utilities', () => {
  it('generates distinct opaque tokens', () => {
    const first = createOpaqueToken();
    const second = createOpaqueToken();

    expect(first).not.toBe(second);
    expect(first.length).toBeGreaterThan(30);
  });

  it('hashes tokens deterministically', () => {
    const token = createOpaqueToken();

    expect(hashToken(token)).toBe(hashToken(token));
    expect(hashToken(token)).not.toBe(token);
  });

  it('compares equal strings', () => {
    expect(
      constantTimeEqual('hookguard', 'hookguard')
    ).toBe(true);
  });

  it('rejects different strings', () => {
    expect(
      constantTimeEqual('hookguard', 'different')
    ).toBe(false);
  });

  it('encrypts and decrypts a secret', () => {
    const original = 'destination-signing-secret';

    const encrypted = encryptSecret(original, key);

    expect(encrypted).not.toContain(original);

    expect(
      decryptSecret(encrypted, key)
    ).toBe(original);
  });

  it('rejects decryption with the wrong key', () => {
    const encrypted = encryptSecret(
      'private-value',
      key
    );

    const wrongKey = Buffer.alloc(32, 7);

    expect(() => {
      decryptSecret(encrypted, wrongKey);
    }).toThrow();
  });
});