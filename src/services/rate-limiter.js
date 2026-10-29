const FIXED_WINDOW_SCRIPT = `
local current = redis.call('INCR', KEYS[1])

if current == 1 then
  redis.call('EXPIRE', KEYS[1], ARGV[1])
end

local ttl = redis.call('TTL', KEYS[1])

return { current, ttl }
`;

export async function consumeRateLimit(
  redis,
  key,
  limit,
  windowSeconds = 60
) {
  const result = await redis.eval(
    FIXED_WINDOW_SCRIPT,
    1,
    key,
    windowSeconds
  );

  const count = Number(result[0]);

  const ttlSeconds = Math.max(
    1,
    Number(result[1])
  );

  return {
    allowed: count <= limit,
    count,
    remaining: Math.max(0, limit - count),
    retryAfterSeconds: ttlSeconds
  };
}