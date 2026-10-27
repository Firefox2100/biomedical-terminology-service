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

  it('falls back to linked status when availability cannot be loaded', async () => {
    document.body.innerHTML = `
      <article data-vocabulary="hpo" data-prefix="hpo"><span data-availability></span></article>`
    vi.stubGlobal('fetch', vi.fn().mockRejectedValue(new Error('offline')))
    await import('@server-js/vocabularies.js')

    await vi.waitFor(() => expect(document.querySelector('[data-availability]'))
      .toHaveTextContent('View status'))
    expect(document.querySelector('[data-availability]').dataset.state).toBe('unavailable')
  })
})
