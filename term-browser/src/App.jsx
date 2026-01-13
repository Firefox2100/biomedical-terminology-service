import './App.css'
import TermBrowser from './components/TermBrowser.jsx'

function App({ ontologyId, rootConceptIds }) {
  return (
    <TermBrowser ontologyId={ontologyId} rootConceptIds={rootConceptIds} />
  )
}

export default App
