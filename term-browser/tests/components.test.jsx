import { render, screen } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { describe, expect, it, vi } from 'vitest'
import DetailsPanel from '../src/components/DetailsPanel.jsx'
import SearchBar from '../src/components/SearchBar.jsx'
import TreeView from '../src/components/TreeView.jsx'

describe('SearchBar', () => {
  it('edits, clears, and selects suggestions with accessible states', async () => {
    const user = userEvent.setup()
    const onChange = vi.fn()
    const onClear = vi.fn()
    const onSelectSuggestion = vi.fn()
    const { rerender } = render(
      <SearchBar value="" onChange={onChange} onClear={onClear} suggestions={[]}
        onSelectSuggestion={onSelectSuggestion} showSuggestions={false} />,
    )

    const input = screen.getByRole('combobox')
    await user.type(input, 'heart')
    expect(onChange).toHaveBeenCalled()
    expect(screen.getAllByRole('button', { name: 'Clear search' })[0]).toBeDisabled()

    rerender(<SearchBar value="heart" onChange={onChange} onClear={onClear}
      suggestions={[{ id: 'HP:1', label: 'Heart disease' }]}
      onSelectSuggestion={onSelectSuggestion} showSuggestions />)
    await user.click(screen.getByRole('option'))
    expect(onSelectSuggestion).toHaveBeenCalledWith({ id: 'HP:1', label: 'Heart disease' })

    input.focus()
    await user.keyboard('{Escape}')
    expect(onClear).toHaveBeenCalled()
  })

  it.each([
    [{ isLoading: true }, 'Searching...'],
    [{ error: 'Search failed' }, 'Search failed'],
    [{ suggestions: [] }, 'No matches found.'],
  ])('renders suggestion state %#', (state, message) => {
    render(<SearchBar value="x" onChange={() => {}} onClear={() => {}}
      suggestions={[{ id: 'x', label: 'X' }]} onSelectSuggestion={() => {}}
      showSuggestions {...state} />)
    expect(screen.getByText(message)).toBeInTheDocument()
  })
})

describe('TreeView', () => {
  const child = { id: 'child', label: 'Child' }
  const root = { id: 'root', label: 'Root', hasChildren: true, children: [child] }

  it('renders empty, loading, expanded, selected, and leaf states', async () => {
    const user = userEvent.setup()
    const onSelect = vi.fn()
    const onToggle = vi.fn()
    const { rerender } = render(<TreeView data={[]} selectedId={null}
      expandedIds={new Set()} loadingIds={new Set()} onSelect={onSelect} onToggle={onToggle} />)
    expect(screen.getByText('No terms to display.')).toBeInTheDocument()

    rerender(<TreeView data={[root]} selectedId="root" expandedIds={new Set(['root'])}
      loadingIds={new Set(['root'])} onSelect={onSelect} onToggle={onToggle} />)
    expect(screen.getByText('Loading...')).toBeInTheDocument()
    expect(screen.getByText('Root').closest('button')).toHaveAttribute('aria-current', 'true')
    await user.click(screen.getByRole('button', { name: 'Collapse Root' }))
    expect(onToggle).toHaveBeenCalledWith(root)

    rerender(<TreeView data={[root]} selectedId="child" expandedIds={new Set(['root'])}
      loadingIds={new Set()} onSelect={onSelect} onToggle={onToggle} />)
    await user.click(screen.getByText('Child').closest('button'))
    expect(onSelect).toHaveBeenCalledWith(child)
    expect(screen.getByRole('button', { name: 'Expand Child' })).toBeDisabled()
  })

  it('shows an expanded node without loaded children', () => {
    render(<TreeView data={[{ id: 'root', label: 'Root', hasChildren: true }]}
      selectedId={null} expandedIds={new Set(['root'])} loadingIds={new Set()}
      onSelect={() => {}} onToggle={() => {}} />)
    expect(screen.getByText('No child terms.')).toBeInTheDocument()
  })
})

describe('DetailsPanel', () => {
  it('renders loading, error, empty, scalar, list, and object values', () => {
    const { rerender } = render(<DetailsPanel isLoading />)
    expect(screen.getByRole('status')).toHaveTextContent('Loading concept details')

    rerender(<DetailsPanel error="offline" />)
    expect(screen.getByRole('alert')).toHaveTextContent('offline')

    rerender(<DetailsPanel />)
    expect(screen.getByText(/Select a concept/)).toBeInTheDocument()

    rerender(<DetailsPanel term={{
      label: 'Seizure', conceptId: 'HP:1', synonyms: ['Fit', 'Convulsion'], empty: [],
      parents: [{ conceptId: 'HP:0', label: 'Root' }], metadata: { active: true },
      nullable: null,
    }} />)
    expect(screen.getByText('Seizure')).toBeInTheDocument()
    expect(screen.getByText('Fit, Convulsion')).toBeInTheDocument()
    expect(screen.getByText('Root')).toBeInTheDocument()
    expect(screen.getByText('{"active":true}')).toBeInTheDocument()
    expect(screen.queryByText('Empty')).not.toBeInTheDocument()
  })
})
