const formatLabel = (key) => key
  .replace(/([a-z0-9])([A-Z])/g, '$1 $2')
  .replace(/[_-]+/g, ' ')
  .replace(/^./, (character) => character.toUpperCase())

function DetailsPanel({ term, isLoading, error, className = '' }) {
  const preferredKeys = ['label', 'conceptId', 'id', 'definition', 'description', 'comment']

  const entries = term
    ? Object.entries(term).filter(([, value]) => value !== null && value !== undefined)
    : []

  const sortedEntries = entries.sort(([keyA], [keyB]) => {
    const indexA = preferredKeys.indexOf(keyA)
    const indexB = preferredKeys.indexOf(keyB)
    if (indexA === -1 && indexB === -1) {
      return keyA.localeCompare(keyB)
    }
    if (indexA === -1) return 1
    if (indexB === -1) return -1
    return indexA - indexB
  })

  const renderValue = (value) => {
    if (Array.isArray(value)) {
      if (!value.length) {
        return null
      }
      if (typeof value[0] === 'object' && value[0] !== null) {
        return (
          <ul className="list-unstyled mb-0">
            {value.map((item) => (
              <li key={item.conceptId || item.id || JSON.stringify(item)}>
                {item.label || item.conceptId || item.id}
              </li>
            ))}
          </ul>
        )
      }
      return <div className="text-muted">{value.join(', ')}</div>
    }

    if (typeof value === 'object') {
      return <div className="text-muted">{JSON.stringify(value)}</div>
    }

    return <div>{String(value)}</div>
  }

  return (
    <div className={`card h-100 ${className}`}>
      <div className="card-header fw-semibold">Concept details</div>
      <div className="card-body">
        {isLoading ? (
          <div className="term-loading" role="status">Loading concept details...</div>
        ) : error ? (
          <div className="alert alert-danger mb-0" role="alert">Concept details could not be loaded. {error}</div>
        ) : !term ? (
          <div className="term-empty">Select a concept from the tree or search to view its details.</div>
        ) : (
          <div className="vstack gap-3">
            {sortedEntries.map(([key, value]) => {
              const rendered = renderValue(value)
              if (!rendered) {
                return null
              }
              return (
                <div key={key}>
                  <div className="term-details__label">{formatLabel(key)}</div>
                  {rendered}
                </div>
              )
            })}
          </div>
        )}
      </div>
    </div>
  )
}

export default DetailsPanel
