(function () {
  const status = document.getElementById('admin-action-status');

  function setStatus(message, isError) {
    if (!status) return;
    status.textContent = message;
    status.classList.toggle('text-danger', Boolean(isError));
  }

  function wireButton(id, pendingText, successMessage, reloadPage) {
    const btn = document.getElementById(id);
    if (!btn) return;

    btn.addEventListener('click', async (e) => {
      e.preventDefault();
      const endpoint = btn.dataset.endpoint;
      const csrf = btn.dataset.csrf || '';

      btn.disabled = true;
      btn.setAttribute('aria-busy', 'true');
      const prevText = btn.textContent;
      btn.textContent = pendingText;
      setStatus(pendingText, false);

      try {
        const res = await fetch(endpoint, {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
            'x-csrf-token': csrf
          },
          body: JSON.stringify({}),
          credentials: 'same-origin'
        });

        if (!res.ok) {
          const text = await res.text();
          setStatus(prevText + ' failed (' + res.status + '). Try again or check the service logs. ' + text, true);
        } else {
          setStatus(successMessage, false);
          if (reloadPage) window.location.reload();
        }
      } catch (err) {
        setStatus('The request could not reach the service. Check your connection and try again.', true);
      } finally {
        btn.disabled = false;
        btn.removeAttribute('aria-busy');
        btn.textContent = prevText;
      }
    });
  }

  wireButton('rebuild-cache-btn', 'Rebuilding...', 'Rebuild started', true);
  wireButton('reload-graphql-btn', 'Reloading...', 'GraphQL schema reload started', false);

  document.querySelectorAll('button[data-confirm]').forEach((button) => {
    button.closest('form')?.addEventListener('submit', (event) => {
      if (!window.confirm(button.dataset.confirm)) {
        event.preventDefault();
      }
    });
  });
}());
