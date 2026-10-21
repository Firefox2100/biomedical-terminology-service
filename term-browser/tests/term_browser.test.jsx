import { render, screen, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { beforeEach, describe, expect, it, vi } from 'vitest'

const service = vi.hoisted(() => ({
  fetchGraphQL: vi.fn(),
  fetchIntrospectionSchema: vi.fn(),
  buildAutoCompleteQuery: vi.fn(() => ({
    query: 'SEARCH', rootFieldName: 'hpo', autoCompleteFieldName: 'autoComplete',
  })),
  buildChildrenQuery: vi.fn(() => ({
    query: 'CHILDREN', rootFieldName: 'hpo', conceptFieldName: 'hpoConcept',
  })),
  buildPathsToQuery: vi.fn(() => null),
  buildTermDetailQuery: vi.fn(() => ({
    query: 'DETAIL', rootFieldName: 'hpo', conceptFieldName: 'hpoConcept',
  })),
}))

vi.mock('../src/services/graphql.js', () => service)

import App from '../src/App.jsx'
import TermBrowser from '../src/components/TermBrowser.jsx'

function detail(data) {
  return { hpo: { hpoConcept: { data } } }
}

beforeEach(() => {
  vi.clearAllMocks()
  service.fetchIntrospectionSchema.mockResolvedValue({ __schema: { queryType: { name: 'Query' } } })
  service.fetchGraphQL.mockImplementation(({ query, variables }) => {
    if (query === 'SEARCH') {
      return Promise.resolve({
        hpo: { autoComplete: { data: [{ conceptId: 'HP:2', label: 'Seizure' }] } },
      })
    }
    if (query === 'CHILDREN') {
      return Promise.resolve(detail({
        children: [{ conceptId: 'HP:2', label: 'Seizure' }],
      }))
    }
    return Promise.resolve(detail({
      conceptId: variables.conceptId,
      label: variables.conceptId === 'HP:1' ? 'Phenotypic abnormality' : 'Seizure',
      definition: 'A clinical finding.',
    }))
  })
})

describe('TermBrowser', () => {
  it('loads roots, expands the tree, and displays selected details', async () => {
    const user = userEvent.setup()
    render(<TermBrowser ontologyId="hpo" rootConceptIds={['HP:1']} />)

    expect(await screen.findByText('Phenotypic abnormality')).toBeInTheDocument()
    await user.click(screen.getByRole('button', { name: 'Expand Phenotypic abnormality' }))
    expect(await screen.findByText('Seizure')).toBeInTheDocument()

    await user.click(screen.getByText('Seizure').closest('button'))
    expect(await screen.findByText('A clinical finding.')).toBeInTheDocument()
    expect(service.fetchGraphQL).toHaveBeenCalledWith(expect.objectContaining({
      query: 'CHILDREN', variables: { conceptId: 'HP:1' },
    }))
  })

  it('searches and selects an autocomplete result', async () => {
    const user = userEvent.setup()
    render(<TermBrowser ontologyId="hpo" rootConceptIds={['HP:1']} />)
    await screen.findByText('Phenotypic abnormality')

    await user.type(screen.getByRole('combobox'), 'seiz')
    const option = await screen.findByRole('option', {}, { timeout: 1500 })
    expect(option).toHaveTextContent('Seizure')
    await user.click(option)

    await waitFor(() => expect(screen.getByRole('combobox')).toHaveValue('Seizure'))
    expect(await screen.findByText('A clinical finding.')).toBeInTheDocument()
    expect(service.fetchGraphQL).toHaveBeenCalledWith(expect.objectContaining({
      query: 'SEARCH', variables: { search: 'seiz' },
    }))
  })

  it('expands the shortest ontology path to a searched concept', async () => {
    const user = userEvent.setup()
    service.buildPathsToQuery.mockReturnValue({
      query: 'PATHS', rootFieldName: 'hpo', conceptFieldName: 'hpoConcept', maxDepth: 10,
    })
    service.fetchGraphQL.mockImplementation(({ query, variables }) => {
      if (query === 'SEARCH') {
        return Promise.resolve({ hpo: { autoComplete: { data: [{ conceptId: 'HP:2', label: 'Seizure' }] } } })
      }
      if (query === 'PATHS') {
        return Promise.resolve(detail({
          pathsTo: [
            { length: 3, nodes: [{ conceptId: 'HP:1' }, { conceptId: 'HP:x' }, { conceptId: 'HP:2' }] },
            { length: 2, nodes: [{ conceptId: 'HP:1' }, { conceptId: 'HP:2' }] },
          ],
        }))
      }
      if (query === 'CHILDREN') {
        return Promise.resolve(detail({ children: [{ conceptId: 'HP:2', label: 'Seizure' }] }))
      }
      return Promise.resolve(detail({ conceptId: variables.conceptId, label: variables.conceptId === 'HP:1' ? 'Root' : 'Seizure' }))
    })
    render(<TermBrowser ontologyId="hpo" rootConceptIds={['HP:1']} />)
    await screen.findByText('Root')

    await user.type(screen.getByRole('combobox'), 'seizure')
    await user.click(await screen.findByRole('option', {}, { timeout: 1500 }))

    expect(await screen.findByRole('button', { name: 'Collapse Root' })).toBeInTheDocument()
    const treeLabel = screen.getAllByText('Seizure').find((element) => element.tagName === 'SPAN')
    expect(treeLabel.closest('button')).toHaveAttribute('aria-current', 'true')
    expect(service.fetchGraphQL).toHaveBeenCalledWith(expect.objectContaining({
      query: 'PATHS', variables: { conceptId: 'HP:1', targetConceptId: 'HP:2', maxDepth: 10 },
    }))
  })

  it('reports tree expansion and detail failures without getting stuck loading', async () => {
    const user = userEvent.setup()
    let detailRequests = 0
    service.fetchGraphQL.mockImplementation(({ query, variables }) => {
      if (query === 'CHILDREN') return Promise.reject(new Error('children offline'))
      detailRequests += 1
      if (detailRequests > 1) return Promise.reject(new Error('detail offline'))
      return Promise.resolve(detail({ conceptId: variables.conceptId, label: 'Root' }))
    })
    render(<TermBrowser ontologyId="hpo" rootConceptIds={['HP:1']} />)
    await screen.findByText('Root')

    await user.click(screen.getByRole('button', { name: 'Expand Root' }))
    expect(await screen.findByText(/concept tree could not be loaded/i)).toHaveTextContent('children offline')
    expect(screen.queryByText('Loading...')).not.toBeInTheDocument()

    await user.click(screen.getByText('Root').closest('button'))
    expect(await screen.findByText(/Concept details could not be loaded/)).toHaveTextContent('detail offline')
  })

  it('renders schema, tree, detail, and search errors', async () => {
    const user = userEvent.setup()
    service.fetchIntrospectionSchema.mockRejectedValueOnce(new Error('schema offline'))
    const firstRender = render(<TermBrowser ontologyId="hpo" rootConceptIds={['HP:1']} />)
    expect(await screen.findByRole('alert')).toHaveTextContent('schema offline')
    firstRender.unmount()

    service.fetchIntrospectionSchema.mockResolvedValue({ __schema: {} })
    service.fetchGraphQL.mockRejectedValue(new Error('backend offline'))
    render(<TermBrowser ontologyId="hpo" rootConceptIds={['HP:1', 'HP:3']} />)
    expect(await screen.findByText(/concept tree could not be loaded/i)).toHaveTextContent('backend offline')

    service.fetchGraphQL.mockImplementation(({ query }) => query === 'SEARCH'
      ? Promise.reject(new Error('search offline'))
      : Promise.resolve(detail({ conceptId: 'HP:1', label: 'Root' })))
    await user.type(screen.getByRole('combobox'), 'abc')
    expect(await screen.findByText('search offline', {}, { timeout: 1500 })).toBeInTheDocument()
  })

  it('shows useful empty states when ontology configuration is absent', async () => {
    const { rerender } = render(<TermBrowser />)
    expect(screen.getByText('Select an ontology to begin.')).toBeInTheDocument()

    rerender(<TermBrowser ontologyId="unknown" rootConceptIds={[]} />)
    expect(await screen.findByText(/Set root concept ids/)).toBeInTheDocument()
  })

  it('passes app-level configuration through to the browser', () => {
    render(<App ontologyId="hpo" rootConceptIds={['HP:1']} />)
    expect(screen.getByRole('heading', { name: 'Ontology term browser' })).toBeInTheDocument()
  })
})
