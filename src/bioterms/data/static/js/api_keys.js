document.addEventListener('DOMContentLoaded', function () {
  const endpointsEl = document.getElementById('api-key-endpoints');
  if (!endpointsEl) {
    console.warn('API Key management: missing #api-key-endpoints element.');
    return;
  }

  const deleteEndpointPattern = endpointsEl.dataset.deleteEndpoint;
  const csrfToken = endpointsEl.dataset.csrfToken;
  const status = document.getElementById('api-key-status');

  function setStatus(message, isError) {
    if (!status) return;
    status.textContent = message;
    status.classList.toggle('text-danger', Boolean(isError));
  }

  if (!deleteEndpointPattern) {
    console.warn('API Key management: missing data-delete-endpoint on #api-key-endpoints.');
    return;
  }

  if (!csrfToken) {
    console.warn('API Key management: missing data-csrf-token on #api-key-endpoints.');
    return;
  }

  const buttons = document.querySelectorAll('.js-revoke-key');

  buttons.forEach(function (btn) {
    btn.addEventListener('click', async function () {
      const keyId = this.dataset.keyId;
      if (!keyId) {
        console.warn('API Key management: revoke button missing data-key-id.');
        return;
      }

      if (!confirm('Revoke this API key? This action cannot be undone.')) {
        return;
      }

      const deleteUrl = deleteEndpointPattern.replace(
        '__KEY__',
        encodeURIComponent(keyId)
      );

      try {
        this.disabled = true;
        this.setAttribute('aria-busy', 'true');
        setStatus('Revoking API key...', false);
        const response = await fetch(deleteUrl, {
          method: 'DELETE',
          headers: {
            'X-Requested-With': 'XMLHttpRequest',
            'x-csrf-token': csrfToken,
          },
          credentials: 'same-origin',
        });

        if (response.ok) {
          const row = this.closest('tr');
          if (row) {
            row.remove();
          }

          setStatus('API key revoked.', false);

          if (!document.querySelector('tbody tr')) {
            window.location.reload();
          }
        } else {
          console.error('Failed to revoke API key. HTTP status:', response.status);
          setStatus('The API key could not be revoked. Try again.', true);
        }
      } catch (err) {
        console.error('Error while revoking API key:', err);
        setStatus('The request could not reach the service. Check your connection and try again.', true);
      } finally {
        this.disabled = false;
        this.removeAttribute('aria-busy');
      }
    });
  });
});
