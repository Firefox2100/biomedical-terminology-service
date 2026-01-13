import './App.css'
import TermBrowser from './components/TermBrowser.jsx'

function App({ ontologyId }) {
  return (
    <TermBrowser ontologyId={ontologyId} />
  )
}

export default App
