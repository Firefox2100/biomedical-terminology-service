import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

beforeEach(() => {
  vi.resetModules()
  vi.restoreAllMocks()
  document.body.innerHTML = ''
})

afterEach(() => {
  vi.unstubAllGlobals()
})

async function loadApiKeysScript() {
  await import('@server-js/api_keys.js')
  document.dispatchEvent(new Event('DOMContentLoaded'))
}

describe('API key management', () => {
  it('does nothing safely when its page data is absent', async () => {
    const warning = vi.spyOn(console, 'warn').mockImplementation(() => {})

    await loadApiKeysScript()

    expect(warning).toHaveBeenCalledWith(
      'API Key management: missing #api-key-endpoints element.',
    )
  })

  it.each([
    ['data-delete-endpoint', '<div id="api-key-endpoints" data-csrf-token="token"></div>'],
    ['data-csrf-token', '<div id="api-key-endpoints" data-delete-endpoint="/api-keys/__KEY__"></div>'],
  ])('requires %s before wiring buttons', async (attribute, markup) => {
    document.body.innerHTML = markup
    const warning = vi.spyOn(console, 'warn').mockImplementation(() => {})

    await loadApiKeysScript()

    expect(warning).toHaveBeenCalledWith(expect.stringContaining(attribute))
  })

  it('ignores malformed buttons and a cancelled confirmation', async () => {
    document.body.innerHTML = `
      <div id="api-key-endpoints" data-delete-endpoint="/api-keys/__KEY__" data-csrf-token="token"></div>
      <button class="js-revoke-key">Missing key</button>
      <button class="js-revoke-key" data-key-id="key-1">Cancel</button>`
    const warning = vi.spyOn(console, 'warn').mockImplementation(() => {})
    const fetch = vi.fn()
    vi.stubGlobal('fetch', fetch)
    vi.stubGlobal('confirm', vi.fn(() => false))
    await loadApiKeysScript()

    const [malformed, cancelled] = document.querySelectorAll('button')
    malformed.click()
    cancelled.click()

    expect(warning).toHaveBeenCalledWith('API Key management: revoke button missing data-key-id.')
    expect(fetch).not.toHaveBeenCalled()
  })

  it('revokes a key and removes its table row', async () => {
    document.body.innerHTML = `
      <div id="api-key-endpoints" data-delete-endpoint="/api-keys/__KEY__" data-csrf-token="token"></div>
      <div id="api-key-status"></div>
      <table><tbody>
        <tr id="target"><td><button class="js-revoke-key" data-key-id="key/1">Revoke</button></td></tr>
        <tr><td>Another key</td></tr>
      </tbody></table>`
    vi.stubGlobal('confirm', vi.fn(() => true))
    const fetch = vi.fn().mockResolvedValue({ ok: true })
    vi.stubGlobal('fetch', fetch)
    await loadApiKeysScript()

    const button = document.querySelector('button')
    button.click()
    await vi.waitFor(() => expect(fetch).toHaveBeenCalled())
    await vi.waitFor(() => expect(document.getElementById('target')).toBeNull())

    expect(fetch).toHaveBeenCalledWith('/api-keys/key%2F1', expect.objectContaining({
      method: 'DELETE',
      credentials: 'same-origin',
    }))
    expect(document.getElementById('api-key-status').textContent).toBe('API key revoked.')
    expect(button.disabled).toBe(false)
    expect(button.hasAttribute('aria-busy')).toBe(false)
  })

  it.each([
    ['an HTTP failure', () => Promise.resolve({ ok: false, status: 503 }), 'could not be revoked'],
    ['a network failure', () => Promise.reject(new Error('offline')), 'could not reach the service'],
  ])('reports %s without leaving the button disabled', async (_label, response, message) => {
    document.body.innerHTML = `
      <div id="api-key-endpoints" data-delete-endpoint="/api-keys/__KEY__" data-csrf-token="token"></div>
      <div id="api-key-status"></div>
      <button class="js-revoke-key" data-key-id="key-1">Revoke</button>`
    vi.stubGlobal('confirm', vi.fn(() => true))
    vi.stubGlobal('fetch', vi.fn().mockImplementation(response))
    vi.spyOn(console, 'error').mockImplementation(() => {})
    await loadApiKeysScript()

    const button = document.querySelector('button')
    button.click()
    await vi.waitFor(() => expect(document.getElementById('api-key-status').textContent).toContain(message))

    expect(document.getElementById('api-key-status').classList).toContain('text-danger')
    expect(button.disabled).toBe(false)
  })
})

describe('site navigation', () => {
  it('opens and closes the responsive navigation', async () => {
    document.body.innerHTML = `
      <button class="nav-toggle" aria-expanded="false">Menu</button>
      <nav id="site-navigation"></nav>`
    await import('@server-js/site.js')

    const toggle = document.querySelector('.nav-toggle')
    const navigation = document.getElementById('site-navigation')
    toggle.click()
    expect(toggle).toHaveAttribute('aria-expanded', 'true')
    expect(navigation).toHaveClass('is-open')
    toggle.click()
    expect(toggle).toHaveAttribute('aria-expanded', 'false')
    expect(navigation).not.toHaveClass('is-open')
  })

  it('is safe on pages without navigation controls', async () => {
    await import('@server-js/site.js')
    expect(document.body).toBeEmptyDOMElement()
  })
})

describe('vocabulary administration', () => {
  it('handles successful and failed actions', async () => {
    document.body.innerHTML = `
      <div id="admin-action-status"></div>
      <button id="rebuild-cache-btn" data-endpoint="/cache" data-csrf="one">Rebuild</button>
      <button id="reload-graphql-btn" data-endpoint="/graphql" data-csrf="two">Reload</button>`
    const fetch = vi.fn()
      .mockResolvedValueOnce({ ok: true })
      .mockResolvedValueOnce({ ok: false, status: 409, text: async () => 'busy' })
    vi.stubGlobal('fetch', fetch)
    await import('@server-js/vocabularies.js')

    const reload = document.getElementById('reload-graphql-btn')
    reload.click()
    await vi.waitFor(() => expect(document.getElementById('admin-action-status').textContent)
      .toBe('GraphQL reload started.'))
    expect(reload.textContent).toBe('Reload')
    expect(reload.disabled).toBe(false)

    const rebuild = document.getElementById('rebuild-cache-btn')
    rebuild.click()
    await vi.waitFor(() => expect(document.getElementById('admin-action-status').textContent)
      .toContain('could not be completed'))
    expect(document.getElementById('admin-action-status').classList).toContain('text-danger')
  })

  it('reports a network error', async () => {
    document.body.innerHTML = `
      <div id="admin-action-status"></div>
      <button id="reload-graphql-btn" data-endpoint="/graphql">Reload</button>`
    vi.stubGlobal('fetch', vi.fn().mockRejectedValue(new Error('offline')))
    await import('@server-js/vocabularies.js')

    document.getElementById('reload-graphql-btn').click()
    await vi.waitFor(() => expect(document.getElementById('admin-action-status').textContent)
      .toContain('could not be completed'))
  })

  it('filters the register and refreshes availability from the API', async () => {
    document.body.innerHTML = `
      <input id="vocabulary-filter">
      <div id="register-count"></div>
      <article data-vocabulary="human phenotype ontology hpo" data-prefix="hpo">
        <span data-availability></span>
      </article>
      <article data-vocabulary="gene ontology go" data-prefix="go">
        <span data-availability></span>
      </article>`
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue({
      ok: true,
      json: async () => [{ prefix: 'hpo', loaded: true }, { prefix: 'go', loaded: false }],
    }))
    await import('@server-js/vocabularies.js')

    await vi.waitFor(() => expect(document.querySelector('[data-prefix="hpo"] [data-availability]'))
      .toHaveTextContent('Available'))
    expect(document.querySelector('[data-prefix="go"] [data-availability]')).toHaveTextContent('Not loaded')

    const filter = document.getElementById('vocabulary-filter')
    filter.value = 'phenotype'
    filter.dispatchEvent(new Event('input'))
    expect(document.querySelector('[data-prefix="hpo"]')).not.toHaveAttribute('hidden')
    expect(document.querySelector('[data-prefix="go"]')).toHaveAttribute('hidden')
    expect(document.getElementById('register-count')).toHaveTextContent('1 vocabulary')
  })

  it('shows an unavailable state when availability cannot be loaded', async () => {
    document.body.innerHTML = `
      <article data-vocabulary="hpo" data-prefix="hpo"><span data-availability></span></article>`
    vi.stubGlobal('fetch', vi.fn().mockRejectedValue(new Error('offline')))
    await import('@server-js/vocabularies.js')

    await vi.waitFor(() => expect(document.querySelector('[data-availability]'))
      .toHaveTextContent('Status unavailable'))
    expect(document.querySelector('[data-availability]').dataset.state).toBe('error')
    expect(document.querySelector('[data-vocabulary]').dataset.availabilityState).toBe('error')
  })
})

function hierarchyMarkup() {
  return `
    <ul data-concept-tree data-api-base="/api/vocabularies/hpo/concepts/">
      <li class="hierarchy-node" data-concept-id="HP:1">
        <div class="hierarchy-node__row">
          <button class="hierarchy-toggle" aria-expanded="false"
            aria-label="Show child terms for Root"><span>toggle</span></button>
        </div>
        <ul class="hierarchy-children" hidden></ul>
      </li>
    </ul>`
}

describe('lazy vocabulary hierarchy', () => {
  it('is safe on pages without a concept tree', async () => {
    await import('@server-js/vocabulary_browser.js')
    expect(document.body).toBeEmptyDOMElement()
  })

  it('loads, renders, opens, and closes child concepts', async () => {
    document.body.innerHTML = hierarchyMarkup()
    const fetch = vi.fn().mockResolvedValue({
      ok: true,
      json: async () => ({ children: [
        { conceptId: 'HP:2', label: 'Child concept' },
        { concept_id: 'HP:3' },
        { conceptId: 'HP:1', label: 'Self cycle' },
        {},
      ] }),
    })
    vi.stubGlobal('fetch', fetch)
    await import('@server-js/vocabulary_browser.js')

    const button = document.querySelector('.hierarchy-toggle')
    button.querySelector('span').click()
    await vi.waitFor(() => expect(document.querySelectorAll('.hierarchy-children > li')).toHaveLength(2))

    expect(fetch).toHaveBeenCalledWith('/api/vocabularies/hpo/concepts/HP%3A1', {
      headers: { Accept: 'application/json' },
    })
    expect(button).toHaveAttribute('aria-expanded', 'true')
    expect(button).toHaveAttribute('aria-label', 'Hide child terms for Root')
    expect(document.querySelector('[data-concept-id="HP:2"] a')).toHaveTextContent('Child conceptHP:2')
    expect(document.querySelector('[data-concept-id="HP:3"] a')).toHaveTextContent('HP:3HP:3')
    expect(document.querySelector('[data-concept-id="HP:2"] a').href).toContain('/HP%3A2')

    button.click()
    expect(button).toHaveAttribute('aria-expanded', 'false')
    expect(document.querySelector('.hierarchy-children')).toHaveAttribute('hidden')

    button.click()
    expect(fetch).toHaveBeenCalledTimes(1)
    expect(button).toHaveAttribute('aria-expanded', 'true')
  })

  it.each([
    [{ children: [] }],
    [{ children: 'not-an-array' }],
  ])('marks a concept as a leaf when no children are returned', async (payload) => {
    document.body.innerHTML = hierarchyMarkup()
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue({ ok: true, json: async () => payload }))
    await import('@server-js/vocabulary_browser.js')

    const button = document.querySelector('.hierarchy-toggle')
    button.click()
    await vi.waitFor(() => expect(button).toHaveClass('is-leaf'))

    expect(button).toBeDisabled()
    expect(button).toHaveAttribute('aria-label', 'No child terms')
    expect(button).not.toHaveAttribute('aria-expanded')
  })

  it('shows an error and permits a retry after a failed request', async () => {
    document.body.innerHTML = hierarchyMarkup()
    const fetch = vi.fn()
      .mockResolvedValueOnce({ ok: false, status: 503 })
      .mockResolvedValueOnce({
        ok: true,
        json: async () => ({ children: [{ id: 'HP:4', label: 'Recovered child' }] }),
      })
    vi.stubGlobal('fetch', fetch)
    await import('@server-js/vocabulary_browser.js')

    const button = document.querySelector('.hierarchy-toggle')
    button.click()
    await vi.waitFor(() => expect(document.querySelector('.hierarchy-message'))
      .toHaveTextContent('could not be loaded'))
    expect(button).not.toBeDisabled()
    expect(document.querySelector('.hierarchy-node')).not.toHaveAttribute('aria-busy')

    button.click()
    await vi.waitFor(() => expect(document.querySelector('[data-concept-id="HP:4"]')).toBeInTheDocument())
    expect(document.querySelector('.hierarchy-message')).not.toBeInTheDocument()
    expect(fetch).toHaveBeenCalledTimes(2)
  })

  it('ignores clicks outside hierarchy toggles and on known leaves', async () => {
    document.body.innerHTML = hierarchyMarkup()
    const fetch = vi.fn()
    vi.stubGlobal('fetch', fetch)
    await import('@server-js/vocabulary_browser.js')

    document.querySelector('[data-concept-tree]').click()
    const button = document.querySelector('.hierarchy-toggle')
    button.classList.add('is-leaf')
    button.click()
    expect(fetch).not.toHaveBeenCalled()
  })
})
