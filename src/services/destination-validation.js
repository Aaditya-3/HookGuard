import { lookup } from 'node:dns/promises';
import { isIP } from 'node:net';
import ipaddr from 'ipaddr.js';

import { AppError } from '../lib/errors.js';

function isPublicAddress(address) {
  let parsed;

  try {
    parsed = ipaddr.parse(address);
  } catch {
    return false;
  }

  if (
    parsed.kind() === 'ipv6' &&
    parsed.isIPv4MappedAddress()
  ) {
    parsed = parsed.toIPv4Address();
  }

  return parsed.range() === 'unicast';
}

export async function validateDestinationUrl(
  input,
  {
    allowPrivate = false,
    requireHttps = false
  } = {}
) {
  let url;

  try {
    url = new URL(input);
  } catch {
    throw new AppError(
      400,
      'INVALID_DESTINATION_URL',
      'Destination URL must be valid'
    );
  }

  if (!['http:', 'https:'].includes(url.protocol)) {
    throw new AppError(
      400,
      'INVALID_DESTINATION_PROTOCOL',
      'Destination URL must use HTTP or HTTPS'
    );
  }

  if (requireHttps && url.protocol !== 'https:') {
    throw new AppError(
      400,
      'HTTPS_REQUIRED',
      'HTTPS is required for destination URLs'
    );
  }

  if (url.username || url.password || url.hash) {
    throw new AppError(
      400,
      'INVALID_DESTINATION_URL',
      'Destination URL cannot contain credentials or fragments'
    );
  }

  const hostname = url.hostname
    .toLowerCase()
    .replace(/\.$/, '');

  if (!hostname) {
    throw new AppError(
      400,
      'INVALID_DESTINATION_URL',
      'Destination hostname is required'
    );
  }

  if (
    !allowPrivate &&
    (
      hostname === 'localhost' ||
      hostname.endsWith('.localhost') ||
      hostname.endsWith('.local') ||
      hostname.endsWith('.internal')
    )
  ) {
    throw new AppError(
      400,
      'PRIVATE_DESTINATION_BLOCKED',
      'Local and internal destinations are not allowed'
    );
  }

  if (!allowPrivate) {
    let addresses;

    try {
      addresses = isIP(hostname)
        ? [{ address: hostname }]
        : await lookup(hostname, {
            all: true,
            verbatim: true
          });
    } catch {
      throw new AppError(
        400,
        'DESTINATION_DNS_FAILED',
        'Destination hostname could not be resolved'
      );
    }

    if (
      addresses.length === 0 ||
      addresses.some(
        ({ address }) => !isPublicAddress(address)
      )
    ) {
      throw new AppError(
        400,
        'PRIVATE_DESTINATION_BLOCKED',
        'Destination resolves to a non-public address'
      );
    }
  }

  return url.toString();
}