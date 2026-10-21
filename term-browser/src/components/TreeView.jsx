function ChevronIcon({ expanded }) {
  return (
    <svg className="term-tree__chevron" viewBox="0 0 16 16" aria-hidden="true">
      <path d={expanded ? 'M3 6l5 5 5-5' : 'M6 3l5 5-5 5'} />
    </svg>
  )
}

function TreeNode({
  node,
  selectedId,
  expandedIds,
  loadingIds,
  onSelect,
  onToggle,
}) {
  const hasChildren = node.hasChildren || (Array.isArray(node.children) && node.children.length > 0)
  const isActive = selectedId === node.id
  const isExpanded = expandedIds.has(node.id)
  const isLoading = loadingIds.has(node.id)

  return (
    <li className="term-tree__item">
      <div className="d-flex align-items-start gap-2">
        <button
          type="button"
          className="term-tree__toggle btn btn-sm btn-light"
          onClick={(event) => {
            event.stopPropagation()
            onToggle(node)
          }}
          aria-label={`${isExpanded ? 'Collapse' : 'Expand'} ${node.label}`}
          aria-expanded={hasChildren ? isExpanded : undefined}
          disabled={!hasChildren}
        >
          {hasChildren ? <ChevronIcon expanded={isExpanded} /> : <span aria-hidden="true" />}
        </button>
        <button
          type="button"
          className={`term-tree__button btn btn-sm w-100 text-start ${
            isActive ? 'is-active' : ''
          }`}
          onClick={() => onSelect(node)}
          aria-current={isActive ? 'true' : undefined}
        >
          <div className="d-flex justify-content-between align-items-start">
            <span>{node.label}</span>
            <span className="term-tree__meta text-muted">{node.id}</span>
          </div>
        </button>
      </div>
      {isExpanded ? (
        <div className="mt-2 ms-3">
          {isLoading ? (
            <div className="text-muted small">Loading...</div>
          ) : Array.isArray(node.children) && node.children.length ? (
            <ul className="term-tree__group list-unstyled">
              {node.children.map((child) => (
                <TreeNode
                  key={child.id}
                  node={child}
                  selectedId={selectedId}
                  expandedIds={expandedIds}
                  loadingIds={loadingIds}
                  onSelect={onSelect}
                  onToggle={onToggle}
                />
              ))}
            </ul>
          ) : (
            <div className="text-muted small">No child terms.</div>
          )}
        </div>
      ) : null}
    </li>
  )
}

function TreeView({ data, selectedId, expandedIds, loadingIds, onSelect, onToggle }) {
  if (!data.length) {
    return <div className="text-muted">No terms to display.</div>
  }

  return (
    <ul className="list-unstyled mb-0" aria-label="Ontology concepts">
      {data.map((node) => (
        <TreeNode
          key={node.id}
          node={node}
          selectedId={selectedId}
          expandedIds={expandedIds}
          loadingIds={loadingIds}
          onSelect={onSelect}
          onToggle={onToggle}
        />
      ))}
    </ul>
  )
}

export default TreeView
