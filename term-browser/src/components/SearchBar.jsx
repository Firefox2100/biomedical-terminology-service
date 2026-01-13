function SearchBar({
  value,
  onChange,
  onClear,
  suggestions,
  onSelectSuggestion,
  isLoading,
  error,
  showSuggestions,
}) {
  const handleSubmit = (event) => {
    event.preventDefault()
  }

  return (
    <form className="d-flex flex-column flex-md-row gap-2" onSubmit={handleSubmit}>
      <div className="input-group flex-grow-1 position-relative">
        <input
          type="search"
          className="form-control"
          placeholder="Search ontology terms"
          aria-label="Search ontology terms"
          value={value}
          onChange={(event) => onChange(event.target.value)}
        />
        <button
          type="button"
          className="btn btn-outline-secondary d-none d-md-inline-flex"
          onClick={onClear}
          disabled={!value}
        >
          Clear
        </button>
        {showSuggestions ? (
          <div className="term-search__menu list-group shadow-sm">
            {isLoading ? (
              <div className="list-group-item text-muted">Searching...</div>
            ) : error ? (
              <div className="list-group-item text-danger">{error}</div>
            ) : suggestions?.length ? (
              suggestions.map((item) => (
                <button
                  key={item.id}
                  type="button"
                  className="list-group-item list-group-item-action"
                  onClick={() => onSelectSuggestion(item)}
                >
                  <div className="fw-semibold">{item.label}</div>
                  <div className="text-muted small">{item.id}</div>
                </button>
              ))
            ) : (
              <div className="list-group-item text-muted">No matches found.</div>
            )}
          </div>
        ) : null}
      </div>
      <button
        type="button"
        className="btn btn-outline-secondary d-md-none"
        onClick={onClear}
        disabled={!value}
      >
        Clear
      </button>
    </form>
  )
}

export default SearchBar
