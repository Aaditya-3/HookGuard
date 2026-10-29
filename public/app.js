const tokenInput = document.querySelector('#admin-token');
const saveTokenButton = document.querySelector('#save-token');

const integrationForm = document.querySelector('#integration-form');
const createResult = document.querySelector('#create-result');

const integrationsList = document.querySelector('#integrations-list');
const deliveriesBody = document.querySelector('#deliveries-body');

const notice = document.querySelector('#notice');

let adminToken = '';

function showNotice(message, isError = false) {
  notice.textContent = message;
  notice.classList.toggle('error', isError);
}

function setText(element, text) {
  element.textContent = text ?? '';
}

async function apiRequest(path, options = {}) {
  const headers = new Headers(options.headers ?? {});

  headers.set('X-Admin-Token', adminToken);

  if (options.body !== undefined) {
    headers.set('Content-Type', 'application/json');
  }

  const response = await fetch(path, {
    ...options,
    headers
  });

  const text = await response.text();

  let body;

  try {
    body = text ? JSON.parse(text) : {};
  } catch {
    body = {
      error: {
        message: 'Server returned a non-JSON response'
      }
    };
  }

  if (!response.ok) {
    throw new Error(
      body?.error?.message ?? `HTTP ${response.status}`
    );
  }

  return body;
}

function createElement(tag, text, className = '') {
  const element = document.createElement(tag);

  if (text !== undefined && text !== null) {
    element.textContent = String(text);
  }

  if (className) {
    element.className = className;
  }

  return element;
}

saveTokenButton.addEventListener('click', async () => {
  adminToken = tokenInput.value.trim();

  if (!adminToken) {
    showNotice('Enter the admin API token first.', true);
    return;
  }

  showNotice('Token set for this browser session.');

  try {
    await refreshIntegrations();
    await refreshDeliveries();
  } catch (error) {
    showNotice(error.message, true);
  }
});

integrationForm.addEventListener('submit', async (event) => {
  event.preventDefault();

  if (!adminToken) {
    showNotice('Set the admin API token first.', true);
    return;
  }

  const formData = new FormData(integrationForm);

  const eventTypes = String(
    formData.get('eventTypes')
  )
    .split(',')
    .map((value) => value.trim())
    .filter(Boolean);

  const input = {
    name: String(formData.get('name')).trim(),

    sourceService: String(
      formData.get('sourceService')
    ).trim(),

    eventTypes,

    destinationUrl: String(
      formData.get('destinationUrl')
    ).trim()
  };

  try {
    const result = await apiRequest(
      '/api/integrations',
      {
        method: 'POST',
        body: JSON.stringify(input)
      }
    );

    createResult.replaceChildren();

    const heading = createElement(
      'h3',
      'Integration created'
    );

    const ingressLabel = createElement(
      'p',
      'Ingress URL (copy it now)'
    );

    const ingressCode = createElement(
      'code',
      result.ingressUrl
    );

    const secretLabel = createElement(
      'p',
      'Destination signing secret (shown only once)'
    );

    const secretCode = createElement(
      'code',
      result.destinationSigningSecret
    );

    const warning = createElement(
      'p',
      'Save the signing secret securely. It will not be returned by the integrations list API.'
    );

    createResult.append(
      heading,
      ingressLabel,
      ingressCode,
      secretLabel,
      secretCode,
      warning
    );

    createResult.classList.remove('hidden');

    integrationForm.reset();

    await refreshIntegrations();

    showNotice(
      'Integration created. Configure the source service to send events to the ingress URL.'
    );
  } catch (error) {
    showNotice(error.message, true);
  }
});

async function refreshIntegrations() {
  const result = await apiRequest('/api/integrations');

  integrationsList.replaceChildren();

  if (!result.integrations.length) {
    integrationsList.append(
      createElement('p', 'No integrations yet.', 'muted')
    );

    return;
  }

  for (const integration of result.integrations) {
    const card = createElement('article', undefined, 'card');

    card.append(
      createElement('h3', integration.name),
      createElement(
        'p',
        `Source: ${integration.sourceService}`
      ),
      createElement(
        'p',
        `Events: ${integration.eventTypes.join(', ')}`
      ),
      createElement(
        'p',
        `Destination: ${integration.destinationUrl}`
      ),
      createElement(
        'p',
        `Status: ${integration.status}`
      ),
      createElement(
        'p',
        `ID: ${integration.id}`
      )
    );

    const button = createElement(
      'button',
      integration.status === 'active'
        ? 'Disable'
        : 'Enable'
    );

    button.type = 'button';

    button.addEventListener('click', async () => {
      try {
        await apiRequest(
          `/api/integrations/${integration.id}/status`,
          {
            method: 'PATCH',
            body: JSON.stringify({
              status: integration.status === 'active'
                ? 'disabled'
                : 'active'
            })
          }
        );

        await refreshIntegrations();

        showNotice('Integration status updated.');
      } catch (error) {
        showNotice(error.message, true);
      }
    });

    card.append(button);
    integrationsList.append(card);
  }
}

async function refreshDeliveries() {
  const result = await apiRequest('/api/deliveries');

  deliveriesBody.replaceChildren();

  if (!result.deliveries.length) {
    const row = document.createElement('tr');
    const cell = createElement(
      'td',
      'No deliveries yet.',
      'muted'
    );

    cell.colSpan = 6;

    row.append(cell);
    deliveriesBody.append(row);

    return;
  }

  for (const delivery of result.deliveries) {
    const row = document.createElement('tr');

    row.append(
      createElement('td', delivery.id),
      createElement('td', delivery.event_type)
    );

    const statusCell = document.createElement('td');

    statusCell.append(
      createElement(
        'span',
        delivery.status,
        `status ${delivery.status}`
      )
    );

    row.append(statusCell);

    row.append(
      createElement(
        'td',
        `${delivery.attempt_count} total`
      ),
      createElement(
        'td',
        new Date(delivery.updated_at).toLocaleString()
      )
    );

    const actionCell = document.createElement('td');

    const inspectButton = createElement(
      'button',
      'Inspect'
    );

    inspectButton.type = 'button';

    inspectButton.addEventListener('click', async () => {
      try {
        const detail = await apiRequest(
          `/api/deliveries/${delivery.id}`
        );

        window.alert(
          JSON.stringify(detail, null, 2)
        );
      } catch (error) {
        showNotice(error.message, true);
      }
    });

    actionCell.append(inspectButton);

    if (delivery.status === 'dead_letter') {
      const replayButton = createElement(
        'button',
        'Replay'
      );

      replayButton.type = 'button';

      replayButton.addEventListener('click', async () => {
        try {
          await apiRequest(
            `/api/deliveries/${delivery.id}/replay`,
            {
              method: 'POST',
              body: JSON.stringify({})
            }
          );

          await refreshDeliveries();

          showNotice('Replay scheduled.');
        } catch (error) {
          showNotice(error.message, true);
        }
      });

      actionCell.append(replayButton);
    }

    row.append(actionCell);
    deliveriesBody.append(row);
  }
}

document
  .querySelector('#refresh-integrations')
  .addEventListener('click', async () => {
    try {
      await refreshIntegrations();
      showNotice('Integrations refreshed.');
    } catch (error) {
      showNotice(error.message, true);
    }
  });

document
  .querySelector('#refresh-deliveries')
  .addEventListener('click', async () => {
    try {
      await refreshDeliveries();
      showNotice('Deliveries refreshed.');
    } catch (error) {
      showNotice(error.message, true);
    }
  });