import {
  createCipheriv,
  createDecipheriv,
  createHash,
  randomBytes,
  timingSafeEqual
} from 'node:crypto';

export function createOpaqueToken(bytes = 32) {
  return randomBytes(bytes).toString('base64url');
}

export function hashToken(token) {
  return createHash('sha256')
    .update(token, 'utf8')
    .digest('hex');
}

export function constantTimeEqual(first, second) {
  const left = Buffer.from(String(first), 'utf8');
  const right = Buffer.from(String(second), 'utf8');

  if (left.length !== right.length) {
    return false;
  }

  return timingSafeEqual(left, right);
}

export function encryptSecret(plaintext, key) {
  if (!Buffer.isBuffer(key) || key.length !== 32) {
    throw new Error('Encryption key must be exactly 32 bytes');
  }

  const iv = randomBytes(12);

  const cipher = createCipheriv(
    'aes-256-gcm',
    key,
    iv
  );

  const ciphertext = Buffer.concat([
    cipher.update(plaintext, 'utf8'),
    cipher.final()
  ]);

  const tag = cipher.getAuthTag();

  return [
    'v1',
    iv.toString('hex'),
    tag.toString('hex'),
    ciphertext.toString('hex')
  ].join(':');
}

export function decryptSecret(encrypted, key) {
  if (!Buffer.isBuffer(key) || key.length !== 32) {
    throw new Error('Encryption key must be exactly 32 bytes');
  }

  const parts = encrypted.split(':');

  if (parts.length !== 4 || parts[0] !== 'v1') {
    throw new Error('Unsupported encrypted-secret format');
  }

  const [, ivHex, tagHex, ciphertextHex] = parts;

  const iv = Buffer.from(ivHex, 'hex');
  const tag = Buffer.from(tagHex, 'hex');
  const ciphertext = Buffer.from(ciphertextHex, 'hex');

  if (iv.length !== 12 || tag.length !== 16) {
    throw new Error('Invalid encrypted-secret structure');
  }

  const decipher = createDecipheriv(
    'aes-256-gcm',
    key,
    iv
  );

  decipher.setAuthTag(tag);

  const plaintext = Buffer.concat([
    decipher.update(ciphertext),
    decipher.final()
  ]);

  return plaintext.toString('utf8');
}