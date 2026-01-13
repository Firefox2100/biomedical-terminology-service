function SearchBar({ value, onChange, onClear }) {
  const handleSubmit = (event) => {
    event.preventDefault()
  }

  return (
    <form className="d-flex flex-column flex-md-row gap-2" onSubmit={handleSubmit}>
      <div className="input-group flex-grow-1">
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
