function DetailsPanel({ term, className = '' }) {
  return (
    <div className={`card h-100 ${className}`}>
      <div className="card-header bg-white fw-semibold">Concept Details</div>
      <div className="card-body">
        {!term ? (
          <div className="text-muted">No concept selected.</div>
        ) : (
          <div className="vstack gap-3">
            <div>
              <div className="term-details__label">Label</div>
              <div className="fs-5 fw-semibold">{term.label}</div>
            </div>
            <div>
              <div className="term-details__label">Identifier</div>
              <div>{term.id}</div>
            </div>
            <div>
              <div className="term-details__label">Description</div>
              <div className="text-muted">
                {term.description || 'No description available.'}
              </div>
            </div>
          </div>
        )}
      </div>
    </div>
  )
}

export default DetailsPanel
