import {
  createHmac,
  timingSafeEqual
} from 'node:crypto';

export function signPayload(secret, timestamp, body) {
  const rawBody = Buffer.isBuffer(body)
    ? body
    : Buffer.from(body, 'utf8');

  const signature = createHmac('sha256', secret)
    .update(`${timestamp}.`, 'utf8')
    .update(rawBody)
    .digest('hex');

  return `v1=${signature}`;
}

export function verifyPayloadSignature({
  secret,
  timestamp,
  body,
  signature,
  maxSkewSeconds = 300,
  nowSeconds = Math.floor(Date.now() / 1000)
}) {
  if (
    !Number.isSafeInteger(Number(timestamp)) ||
    typeof signature !== 'string'
  ) {
    return false;
  }

  const timestampSeconds = Number(timestamp);

  if (
    Math.abs(nowSeconds - timestampSeconds) >
    maxSkewSeconds
  ) {
    return false;
  }

  const supplied = signature.startsWith('v1=')
    ? signature.slice(3)
    : '';

  if (!/^[a-fA-F0-9]{64}$/.test(supplied)) {
    return false;
  }

  const expected = signPayload(
    secret,
    String(timestampSeconds),
    body
  ).slice(3);

  const suppliedBuffer = Buffer.from(supplied, 'hex');
  const expectedBuffer = Buffer.from(expected, 'hex');

  if (suppliedBuffer.length !== expectedBuffer.length) {
    return false;
  }

  return timingSafeEqual(
    suppliedBuffer,
    expectedBuffer
  );
}