import './App.css'
import TermBrowser from './components/TermBrowser.jsx'

function App({ ontologyId, rootConceptIds, returnPath }) {
  return (
    <TermBrowser
      ontologyId={ontologyId}
      rootConceptIds={rootConceptIds}
      returnPath={returnPath}
    />
  )
}

export default App
