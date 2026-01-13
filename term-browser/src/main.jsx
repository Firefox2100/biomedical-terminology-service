import { StrictMode } from 'react'
import { createRoot } from 'react-dom/client'
import './index.css'
import App from './App.jsx'

const params = new URLSearchParams(window.location.search)
const ontologyId = params.get('ontology')
const rootParams = params.getAll('root')
const rootConceptIds = rootParams.flatMap((value) =>
  value.split(',').map((entry) => entry.trim()),
).filter(Boolean)

createRoot(document.getElementById('root')).render(
  <StrictMode>
    <App ontologyId={ontologyId} rootConceptIds={rootConceptIds} />
  </StrictMode>,
)
