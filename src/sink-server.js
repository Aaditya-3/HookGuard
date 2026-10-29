import express from 'express';
import { randomUUID } from 'node:crypto';

const app = express();

const port = Number.parseInt(
  process.env.SINK_PORT ?? '4001',
  10
);

app.use(express.json({ limit: '256kb' }));

let mode = 'success';
let latencyMs = 0;
let failuresRemaining = 0;

const receivedEvents = [];

app.get('/health/live', (req, res) => {
  res.status(200).json({
    status: 'ok'
  });
});

app.get('/stats', (req, res) => {
  res.status(200).json({
    mode,
    latencyMs,
    failuresRemaining,
    receivedCount: receivedEvents.length,
    events: receivedEvents.slice(-100)
  });
});

// Local development control endpoint.
// Do not expose this endpoint publicly.
app.post('/__control', (req, res) => {
  const {
    newMode,
    newLatencyMs,
    newFailuresRemaining
  } = req.body ?? {};

  const allowedModes = [
    'success',
    'http_error',
    'fail_n',
    'delay',
    'disconnect'
  ];

  if (
    newMode !== undefined &&
    !allowedModes.includes(newMode)
  ) {
    return res.status(400).json({
      error: 'Invalid sink mode'
    });
  }

  if (newMode !== undefined) {
    mode = newMode;
  }

  if (newLatencyMs !== undefined) {
    if (
      !Number.isInteger(newLatencyMs) ||
      newLatencyMs < 0 ||
      newLatencyMs > 60000
    ) {
      return res.status(400).json({
        error: 'newLatencyMs must be between 0 and 60000'
      });
    }

    latencyMs = newLatencyMs;
  }

  if (newFailuresRemaining !== undefined) {
    if (
      !Number.isInteger(newFailuresRemaining) ||
      newFailuresRemaining < 0
    ) {
      return res.status(400).json({
        error: 'newFailuresRemaining must be nonnegative'
      });
    }

    failuresRemaining = newFailuresRemaining;
  }

  return res.status(200).json({
    mode,
    latencyMs,
    failuresRemaining
  });
});

app.post('/__reset', (req, res) => {
  mode = 'success';
  latencyMs = 0;
  failuresRemaining = 0;

  receivedEvents.length = 0;

  res.status(200).json({
    status: 'reset'
  });
});

app.post('/webhook', async (req, res) => {
  const event = {
    receivedId: randomUUID(),
    eventId: req.get('x-hookguard-event-id') ?? null,
    eventType: req.get('x-hookguard-event-type') ?? null,
    receivedAt: new Date().toISOString(),
    body: req.body
  };

  receivedEvents.push(event);

  if (receivedEvents.length > 10000) {
    receivedEvents.shift();
  }

  if (latencyMs > 0) {
    await new Promise((resolve) => {
      setTimeout(resolve, latencyMs);
    });
  }

  if (mode === 'disconnect') {
    req.socket.destroy();
    return;
  }

  if (mode === 'http_error') {
    return res.status(500).json({
      error: 'Simulated server failure'
    });
  }

  if (
    mode === 'fail_n' &&
    failuresRemaining > 0
  ) {
    failuresRemaining -= 1;

    return res.status(503).json({
      error: 'Simulated temporary failure',
      failuresRemaining
    });
  }

  return res.status(200).json({
    received: true,
    eventId: event.eventId
  });
});

app.listen(port, '0.0.0.0', () => {
  console.log(
    `HookGuard test sink listening on port ${port}`
  );
});