(function () {
  const status = document.getElementById('admin-action-status');

  function setStatus(message, isError) {
    if (!status) return;
    status.textContent = message;
    status.classList.toggle('text-danger', Boolean(isError));
  }

  function wireButton(id, pendingText, successMessage, reloadPage) {
    const button = document.getElementById(id);
    if (!button) return;
    button.addEventListener('click', async function (event) {
      event.preventDefault();
      const previous = button.textContent;
      button.disabled = true;
      button.textContent = pendingText;
      try {
        const response = await fetch(button.dataset.endpoint, {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
            'x-csrf-token': button.dataset.csrf || '',
          },
          body: '{}',
          credentials: 'same-origin',
        });
        if (!response.ok) throw new Error(String(response.status));
        setStatus(successMessage, false);
        if (reloadPage) window.location.reload();
      } catch (_) {
        setStatus('The action could not be completed. Try again or check the service logs.', true);
      } finally {
        button.disabled = false;
        button.textContent = previous;
      }
    });
  }

  wireButton('rebuild-cache-btn', 'Rebuilding...', 'Cache rebuild started.', true);
  wireButton('reload-graphql-btn', 'Reloading...', 'GraphQL reload started.', false);

  const filter = document.getElementById('vocabulary-filter');
  const rows = Array.from(document.querySelectorAll('[data-vocabulary]'));
  const count = document.getElementById('register-count');

  if (filter) {
    filter.addEventListener('input', function () {
      const query = filter.value.trim().toLowerCase();
      let visible = 0;
      rows.forEach(function (row) {
        const show = row.dataset.vocabulary.includes(query);
        row.hidden = !show;
        if (show) visible += 1;
      });
      if (count) count.textContent = visible + (visible === 1 ? ' vocabulary' : ' vocabularies');
    });
  }

  function setAvailability(row, state, label) {
    const target = row.querySelector('[data-availability]');
    row.dataset.availabilityState = state;
    if (!target) return;
    target.textContent = label;
    target.dataset.state = state;
  }

  if (rows.length) {
    fetch('/api/vocabularies', { headers: { Accept: 'application/json' } })
      .then(function (response) {
        if (!response.ok) throw new Error(String(response.status));
        return response.json();
      })
      .then(function (items) {
        const availability = new Map(items.map(function (item) {
          return [item.prefix, item.loaded];
        }));
        rows.forEach(function (row) {
          const loaded = availability.get(row.dataset.prefix);
          setAvailability(row, loaded ? 'available' : 'unavailable', loaded ? 'Available' : 'Not loaded');
        });
      })
      .catch(function () {
        rows.forEach(function (row) {
          setAvailability(row, 'error', 'Status unavailable');
        });
      });
  }
})();
