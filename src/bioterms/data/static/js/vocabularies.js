(function () {
  function wireButton(id, pendingText, successMessage, reloadPage) {
    const btn = document.getElementById(id);
    if (!btn) return;

    btn.addEventListener('click', async (e) => {
      e.preventDefault();
      const endpoint = btn.dataset.endpoint;
      const csrf = btn.dataset.csrf || '';

      btn.disabled = true;
      const prevText = btn.textContent;
      btn.textContent = pendingText;

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
          alert(prevText + ' failed: ' + res.status + ' — ' + text);
        } else {
          alert(successMessage);
          if (reloadPage) window.location.reload();
        }
      } catch (err) {
        alert('Network error: ' + err);
      } finally {
        btn.disabled = false;
        btn.textContent = prevText;
      }
    });
  }

  wireButton('rebuild-cache-btn', 'Rebuilding...', 'Rebuild started', true);
  wireButton('reload-graphql-btn', 'Reloading...', 'GraphQL schema reload started', false);
}());
