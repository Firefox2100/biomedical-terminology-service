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

describe('vocabulary administration', () => {
  it('handles successful and failed actions and confirmation forms', async () => {
    document.body.innerHTML = `
      <div id="admin-action-status"></div>
      <button id="rebuild-cache-btn" data-endpoint="/cache" data-csrf="one">Rebuild</button>
      <button id="reload-graphql-btn" data-endpoint="/graphql" data-csrf="two">Reload</button>
      <form><button data-confirm="Continue?">Delete</button></form>`
    const fetch = vi.fn()
      .mockResolvedValueOnce({ ok: true })
      .mockResolvedValueOnce({ ok: false, status: 409, text: async () => 'busy' })
    vi.stubGlobal('fetch', fetch)
    vi.stubGlobal('confirm', vi.fn(() => false))
    await import('@server-js/vocabularies.js')

    const reload = document.getElementById('reload-graphql-btn')
    reload.click()
    await vi.waitFor(() => expect(document.getElementById('admin-action-status').textContent)
      .toBe('GraphQL schema reload started'))
    expect(reload.textContent).toBe('Reload')
    expect(reload.disabled).toBe(false)

    const rebuild = document.getElementById('rebuild-cache-btn')
    rebuild.click()
    await vi.waitFor(() => expect(document.getElementById('admin-action-status').textContent).toContain('failed (409)'))
    expect(document.getElementById('admin-action-status').textContent).toContain('busy')
    expect(document.getElementById('admin-action-status').classList).toContain('text-danger')

    const form = document.querySelector('form')
    const submit = new Event('submit', { cancelable: true })
    form.dispatchEvent(submit)
    expect(submit.defaultPrevented).toBe(true)
  })

  it('reports a network error', async () => {
    document.body.innerHTML = `
      <div id="admin-action-status"></div>
      <button id="reload-graphql-btn" data-endpoint="/graphql">Reload</button>`
    vi.stubGlobal('fetch', vi.fn().mockRejectedValue(new Error('offline')))
    await import('@server-js/vocabularies.js')

    document.getElementById('reload-graphql-btn').click()
    await vi.waitFor(() => expect(document.getElementById('admin-action-status').textContent)
      .toContain('could not reach the service'))
  })
})
