(function () {
  const btn = document.getElementById('rebuild-cache-btn');
  if (!btn) return;

  btn.addEventListener('click', async (e) => {
    e.preventDefault();
    const endpoint = btn.dataset.endpoint;
    const csrf = btn.dataset.csrf || '';

    btn.disabled = true;
    const prevText = btn.textContent;
    btn.textContent = 'Rebuilding...';

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
        alert('Rebuild failed: ' + res.status + ' — ' + text);
      } else {
        // success feedback — reload to reflect updated cache/state
        alert('Rebuild started');
        window.location.reload();
      }
    } catch (err) {
      alert('Network error: ' + err);
    } finally {
      btn.disabled = false;
      btn.textContent = prevText;
    }
  });
}());
