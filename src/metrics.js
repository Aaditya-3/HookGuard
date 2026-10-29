import {
  Registry,
  Counter,
  Histogram,
  collectDefaultMetrics
} from 'prom-client';

export function createMetrics(serviceName) {
  const registry = new Registry();

  registry.setDefaultLabels({
    service: serviceName
  });

  collectDefaultMetrics({
    register: registry
  });

  const ingressRequests = new Counter({
    name: 'hookguard_ingress_requests_total',
    help: 'Webhook ingress requests by outcome',
    labelNames: ['outcome'],
    registers: [registry]
  });

  const deliveryAttempts = new Counter({
    name: 'hookguard_delivery_attempts_total',
    help: 'Webhook outbound delivery attempts by outcome',
    labelNames: ['outcome'],
    registers: [registry]
  });

  const deliveryDuration = new Histogram({
    name: 'hookguard_delivery_duration_seconds',
    help: 'Duration of outgoing webhook attempts in seconds',
    buckets: [
      0.05,
      0.1,
      0.25,
      0.5,
      1,
      2.5,
      5,
      10,
      20
    ],
    registers: [registry]
  });

  return {
    registry,
    ingressRequests,
    deliveryAttempts,
    deliveryDuration
  };
}