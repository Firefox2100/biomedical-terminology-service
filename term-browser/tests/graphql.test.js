import { beforeEach, describe, expect, it, vi } from 'vitest'
import {
  buildAutoCompleteQuery, buildChildrenPathQuery, buildChildrenQuery,
  buildParentsQuery, buildPathsToQuery, buildTermDetailQuery,
  fetchGraphQL, fetchIntrospectionSchema, getQueryField,
} from '../src/services/graphql.js'

const scalar = (name = 'String') => ({ kind: 'SCALAR', name })
const object = (name) => ({ kind: 'OBJECT', name })
const nonNull = (type) => ({ kind: 'NON_NULL', ofType: type })

const schema = {
  queryType: { name: 'Query' },
  types: [
    { name: 'Query', fields: [{ name: 'hpo', type: object('HpoQueries') }] },
    { name: 'HpoQueries', fields: [
      { name: 'hpoConcept', args: [{ name: 'conceptId', type: nonNull(scalar('String')) }], type: object('ConceptResponse') },
      { name: 'autoComplete', args: [{ name: 'query', type: scalar('String') }], type: object('SearchResponse') },
    ] },
    { name: 'ConceptResponse', fields: [{ name: 'data', type: object('Concept') }] },
    { name: 'Concept', fields: [
      { name: 'conceptId', type: scalar('String') }, { name: 'label', type: scalar('String') },
      { name: 'synonyms', type: { kind: 'LIST', ofType: scalar('String') } },
      { name: 'parents', type: { kind: 'LIST', ofType: object('Concept') } },
      { name: 'children', type: { kind: 'LIST', ofType: object('Concept') } },
      { name: 'pathsTo', args: [
        { name: 'targetPrefix' }, { name: 'relationship' }, { name: 'direction' },
        { name: 'maxDepth' }, { name: 'targetConceptId' },
      ], type: object('Path') },
    ] },
    { name: 'String', kind: 'SCALAR' },
  ],
}

beforeEach(() => vi.restoreAllMocks())

describe('GraphQL transport', () => {
  it('posts requests and returns data', async () => {
    const fetch = vi.spyOn(globalThis, 'fetch').mockResolvedValue({
      ok: true, json: async () => ({ data: { hpo: true } }),
    })
    await expect(fetchGraphQL({ query: 'query X', variables: { id: 1 } }))
      .resolves.toEqual({ hpo: true })
    expect(fetch).toHaveBeenCalledWith(expect.stringContaining('/api/graphql/'), expect.objectContaining({ method: 'POST' }))
  })

  it('surfaces HTTP and GraphQL errors', async () => {
    vi.spyOn(globalThis, 'fetch').mockResolvedValueOnce({ ok: false, status: 503 })
    await expect(fetchGraphQL({ query: 'x' })).rejects.toThrow('503')
    globalThis.fetch.mockResolvedValueOnce({ ok: true, json: async () => ({ errors: [{ message: 'one' }, { message: 'two' }] }) })
    await expect(fetchGraphQL({ query: 'x' })).rejects.toThrow('one; two')
  })

  it('uses the introspection query', async () => {
    vi.spyOn(globalThis, 'fetch').mockResolvedValue({ ok: true, json: async () => ({ data: { __schema: schema } }) })
    await expect(fetchIntrospectionSchema()).resolves.toEqual({ __schema: schema })
    expect(JSON.parse(globalThis.fetch.mock.calls[0][1].body).query).toContain('IntrospectionQuery')
  })
})

describe('GraphQL query builders', () => {
  const input = { schema, ontologyId: 'HPO', fieldOverrides: {} }

  it('resolves ontology fields and overrides case-insensitively', () => {
    expect(getQueryField(schema, 'HPO').name).toBe('hpo')
    expect(getQueryField(schema, 'anything', { anything: 'hpo' }).name).toBe('hpo')
    expect(getQueryField({}, 'hpo')).toBeNull()
  })

  it('builds children, detail, autocomplete, parent, child-path, and path queries', () => {
    const children = buildChildrenQuery(input)
    expect(children.query).toContain('hpoConcept(conceptId: $conceptId)')
    expect(children.query).toContain('$conceptId: String!')

    const detail = buildTermDetailQuery(input)
    expect(detail.query).toContain('synonyms')
    expect(detail.query).toContain('parents { conceptId label }')

    const autoComplete = buildAutoCompleteQuery(input)
    expect(autoComplete.query).toContain('autoComplete(query: $search)')
    expect(autoComplete.searchArgName).toBe('query')

    expect(buildParentsQuery({ ...input, depth: 1 }).query).toContain('parents {')
    expect(buildChildrenPathQuery({ ...input, depth: 1 }).query).toContain('children {')

    const paths = buildPathsToQuery({ ...input, maxDepth: 7 })
    expect(paths.query).toContain('targetPrefix: hpo')
    expect(paths.query).toContain('targetConceptId: $targetConceptId')
    expect(paths.maxDepth).toBe(7)
  })

  it('returns null when required schema capabilities are absent', () => {
    const missing = { queryType: { name: 'Query' }, types: [{ name: 'Query', fields: [] }] }
    const value = { schema: missing, ontologyId: 'hpo' }
    expect(buildChildrenQuery(value)).toBeNull()
    expect(buildTermDetailQuery(value)).toBeNull()
    expect(buildAutoCompleteQuery(value)).toBeNull()
    expect(buildParentsQuery(value)).toBeNull()
    expect(buildChildrenPathQuery(value)).toBeNull()
    expect(buildPathsToQuery(value)).toBeNull()
  })
})
