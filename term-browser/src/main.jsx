import { StrictMode } from 'react'
import { createRoot } from 'react-dom/client'
import './index.css'
import App from './App.jsx'

const params = new URLSearchParams(window.location.search)
const ontologyId = params.get('ontology')

createRoot(document.getElementById('root')).render(
  <StrictMode>
    <App ontologyId={ontologyId} />
  </StrictMode>,
)
