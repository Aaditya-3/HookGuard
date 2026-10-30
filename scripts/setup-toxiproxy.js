const baseUrl = (
  process.env.TOXIPROXY_API_URL ??
  'http://127.0.0.1:8474'
).replace(/\/$/, '');

const proxyName = 'hookguard-sink';

async function apiRequest(path, options = {}) {
  const response = await fetch(`${baseUrl}${path}`, {
    ...options,
    headers: {
      'content-type': 'application/json',
      ...options.headers,
    },
    signal: AbortSignal.timeout(5000),
  });

  const text = await response.text();

  if (!response.ok) {
    throw new Error(
      `Toxiproxy API returned HTTP ${response.status}: ${text}`,
    );
  }

  return text ? JSON.parse(text) : null;
}

async function main() {
  // Verify that Toxiproxy is available.
  const proxies = await apiRequest('/proxies');

  const existing = proxies[proxyName];

  if (existing) {
    console.log(`Removing existing proxy "${proxyName}".`);

    await apiRequest(`/proxies/${proxyName}`, {
      method: 'DELETE',
    });
  }

  await apiRequest('/proxies', {
    method: 'POST',
    body: JSON.stringify({
      name: proxyName,
      listen: '0.0.0.0:8666',
      upstream: 'webhook-sink:4001',
      enabled: true,
    }),
  });

  console.log(
    `Proxy "${proxyName}" configured: ` +
    'toxiproxy:8666 -> webhook-sink:4001',
  );
}

main().catch((error) => {
  console.error(`Toxiproxy setup failed: ${error.message}`);
  process.exitCode = 1;
});