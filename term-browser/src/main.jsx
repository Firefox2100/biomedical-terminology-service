import { StrictMode } from 'react'
import { createRoot } from 'react-dom/client'
import './index.css'
import App from './App.jsx'

function ensureStylesheet({ href, integrity, crossOrigin }) {
  const id = "bootstrap-css";
  let el = document.getElementById(id);

  if (!el) {
    el = document.createElement("link");
    el.id = id;
    el.rel = "stylesheet";
    document.head.appendChild(el);
  }

  el.href = href;

  if (integrity) el.integrity = integrity;
  else el.removeAttribute("integrity");

  if (crossOrigin) el.crossOrigin = crossOrigin;
  else el.removeAttribute("crossorigin");
}

function getSafeReturnPath(value) {
  if (!value) {
    return null;
  }

  const trimmed = value.trim();
  if (!trimmed) {
    return null;
  }

  if (!/^\/vocabularies\/[^/]+$/.test(trimmed)) {
    return null;
  }

  return trimmed;
}

const params = new URLSearchParams(window.location.search)
const ontologyId = params.get('ontology')
const rootParams = params.getAll('root')
const returnPath = getSafeReturnPath(params.get('returnTo'))
const rootConceptIds = rootParams.flatMap((value) =>
  value.split(',').map((entry) => entry.trim()),
).filter(Boolean)

// The compatibility build is fully self-contained. The integrated Jinja browser is the
// public default, but this bundle still uses the locally shipped Bootstrap package when
// opened during migration or development—never a CDN fallback.
ensureStylesheet({
  href: "/static/vendor/bootstrap/5.3.8/css/bootstrap.min.css",
});

createRoot(document.getElementById('root')).render(
  <StrictMode>
    <App
      ontologyId={ontologyId}
      rootConceptIds={rootConceptIds}
      returnPath={returnPath}
    />
  </StrictMode>,
)
