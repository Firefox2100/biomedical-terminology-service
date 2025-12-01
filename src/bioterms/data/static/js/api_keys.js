document.addEventListener('DOMContentLoaded', function () {
  const endpointsEl = document.getElementById('api-key-endpoints');
  if (!endpointsEl) {
    console.warn('API Key management: missing #api-key-endpoints element.');
    return;
  }

  const deleteEndpointPattern = endpointsEl.getAttribute('data-delete-endpoint');
  const csrfToken = endpointsEl.getAttribute('data-csrf-token');

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
      const keyId = this.getAttribute('data-key-id');
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

          if (!document.querySelector('tbody tr')) {
            window.location.reload();
          }
        } else {
          console.error('Failed to revoke API key. HTTP status:', response.status);
          alert('Failed to revoke the API key.');
        }
      } catch (err) {
        console.error('Error while revoking API key:', err);
        alert('An error occurred while revoking the API key.');
      }
    });
  });
});
