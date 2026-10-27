(function () {
  const tree = document.querySelector('[data-concept-tree]');
  if (!tree) return;

  const apiBase = tree.dataset.apiBase;

  function conceptId(concept) {
    return concept.conceptId || concept.concept_id || concept.id || '';
  }

  function conceptUrl(id) {
    const page = new URL(window.location.href);
    page.search = '';
    page.hash = '';
    page.pathname = page.pathname.replace(/\/$/, '') + '/' + encodeURIComponent(id);
    return page.toString();
  }

  function makeCaret() {
    const namespace = 'http://www.w3.org/2000/svg';
    const icon = document.createElementNS(namespace, 'svg');
    icon.setAttribute('class', 'icon');
    icon.setAttribute('viewBox', '0 0 16 16');
    icon.setAttribute('aria-hidden', 'true');
    const path = document.createElementNS(namespace, 'path');
    path.setAttribute('d', 'M5 3l5 5-5 5');
    icon.append(path);
    return icon;
  }

  function makeNode(concept, parentId) {
    const id = conceptId(concept);
    if (!id || id === parentId) return null;

    const item = document.createElement('li');
    item.className = 'hierarchy-node';
    item.dataset.conceptId = id;

    const row = document.createElement('div');
    row.className = 'hierarchy-node__row';

    const toggle = document.createElement('button');
    toggle.className = 'hierarchy-toggle';
    toggle.type = 'button';
    toggle.setAttribute('aria-expanded', 'false');
    toggle.setAttribute('aria-label', 'Show child terms for ' + (concept.label || id));
    toggle.append(makeCaret());

    const link = document.createElement('a');
    link.className = 'hierarchy-node__link';
    link.href = conceptUrl(id);
    const label = document.createElement('strong');
    label.textContent = concept.label || id;
    const code = document.createElement('code');
    code.textContent = id;
    link.append(label, code);

    const children = document.createElement('ul');
    children.className = 'hierarchy-children';
    children.hidden = true;

    row.append(toggle, link);
    item.append(row, children);
    return item;
  }

  function markLeaf(item, button) {
    button.disabled = true;
    button.classList.add('is-leaf');
    button.removeAttribute('aria-expanded');
    button.setAttribute('aria-label', 'No child terms');
    item.dataset.loaded = 'true';
  }

  function showError(container) {
    let message = container.querySelector('.hierarchy-message');
    if (!message) {
      message = document.createElement('li');
      message.className = 'hierarchy-message';
      container.append(message);
    }
    message.textContent = 'Child terms could not be loaded. Try expanding this concept again.';
    container.hidden = false;
  }

  async function loadChildren(item, button, container) {
    button.disabled = true;
    button.classList.add('is-loading');
    item.setAttribute('aria-busy', 'true');
    container.querySelector('.hierarchy-message')?.remove();

    try {
      const id = item.dataset.conceptId;
      const response = await fetch(apiBase + encodeURIComponent(id), {
        headers: { Accept: 'application/json' },
      });
      if (!response.ok) throw new Error(String(response.status));
      const payload = await response.json();
      const children = Array.isArray(payload.children) ? payload.children : [];
      const fragment = document.createDocumentFragment();
      children.forEach(function (concept) {
        const node = makeNode(concept, id);
        if (node) fragment.append(node);
      });

      if (!fragment.childNodes.length) {
        markLeaf(item, button);
        container.hidden = true;
        return false;
      }

      container.replaceChildren(fragment);
      item.dataset.loaded = 'true';
      return true;
    } catch (_) {
      showError(container);
      return false;
    } finally {
      button.disabled = button.classList.contains('is-leaf');
      button.classList.remove('is-loading');
      item.removeAttribute('aria-busy');
    }
  }

  tree.addEventListener('click', async function (event) {
    const button = event.target.closest('.hierarchy-toggle');
    if (!button || button.classList.contains('is-leaf')) return;

    const item = button.closest('.hierarchy-node');
    const container = item.querySelector(':scope > .hierarchy-children');
    const opening = button.getAttribute('aria-expanded') !== 'true';

    if (!opening) {
      button.setAttribute('aria-expanded', 'false');
      button.setAttribute('aria-label', button.getAttribute('aria-label').replace('Hide', 'Show'));
      container.hidden = true;
      return;
    }

    if (item.dataset.loaded !== 'true') {
      const loaded = await loadChildren(item, button, container);
      if (!loaded) return;
    }

    button.setAttribute('aria-expanded', 'true');
    button.setAttribute('aria-label', button.getAttribute('aria-label').replace('Show', 'Hide'));
    container.hidden = false;
  });
})();
