import { useEffect, useMemo, useState } from 'react'
import SearchBar from './SearchBar.jsx'
import TreeView from './TreeView.jsx'
import DetailsPanel from './DetailsPanel.jsx'
import { sampleOntology } from '../data/sampleOntology.js'
import {
  buildOntologyQueryFromSchema,
  fetchIntrospectionSchema,
} from '../services/graphql.js'

const lazyChildren = {
  'DX:101': [
    {
      id: 'DX:101.001',
      label: 'Respiratory Infection',
      description: 'Infections affecting the respiratory tract.',
    },
    {
      id: 'DX:101.002',
      label: 'Gastrointestinal Infection',
      description: 'Infections affecting the gastrointestinal tract.',
    },
  ],
}

function updateNodeById(nodes, nodeId, updater) {
  return nodes.map((node) => {
    if (node.id === nodeId) {
      return updater(node)
    }

    if (node.children?.length) {
      return {
        ...node,
        children: updateNodeById(node.children, nodeId, updater),
      }
    }

    return node
  })
}

function TermBrowser({ ontologyId }) {
  const [query, setQuery] = useState('')
  const [selectedTerm, setSelectedTerm] = useState(null)
  const [treeData, setTreeData] = useState(sampleOntology)
  const [expandedIds, setExpandedIds] = useState(() => new Set())
  const [loadingIds, setLoadingIds] = useState(() => new Set())
  const [schema, setSchema] = useState(null)
  const [schemaError, setSchemaError] = useState(null)

  const filteredData = useMemo(() => treeData, [treeData])

  useEffect(() => {
    let isMounted = true

    async function loadSchema() {
      try {
        setSchemaError(null)
        const data = await fetchIntrospectionSchema()
        if (!isMounted) {
          return
        }

        setSchema(data.__schema)
        buildOntologyQueryFromSchema(data.__schema, ontologyId)
      } catch (error) {
        if (!isMounted) {
          return
        }

        setSchemaError(error.message)
      }
    }

    if (ontologyId) {
      loadSchema()
    }

    return () => {
      isMounted = false
    }
  }, [ontologyId])

  const handleToggle = async (node) => {
    const hasChildren =
      node.hasChildren || (Array.isArray(node.children) && node.children.length > 0)

    if (!hasChildren) {
      return
    }

    const isExpanded = expandedIds.has(node.id)
    setExpandedIds((current) => {
      const next = new Set(current)
      if (isExpanded) {
        next.delete(node.id)
      } else {
        next.add(node.id)
      }
      return next
    })

    if (!isExpanded && !node.children && node.hasChildren) {
      setLoadingIds((current) => new Set(current).add(node.id))

      const children = lazyChildren[node.id] || []
      setTreeData((current) =>
        updateNodeById(current, node.id, (currentNode) => ({
          ...currentNode,
          children,
          hasChildren: children.length > 0,
        })),
      )

      setLoadingIds((current) => {
        const next = new Set(current)
        next.delete(node.id)
        return next
      })
    }
  }

  return (
    <div className="term-browser container-fluid py-4">
      <div className="row g-3">
        <div className="col-12">
          <div className="card shadow-sm">
            <div className="card-body">
              <div className="d-flex flex-wrap justify-content-between align-items-center gap-3">
                <div>
                  <h1 className="h4 mb-1">Ontology Term Browser</h1>
                  <p className="text-muted mb-0">
                    Search and explore terminology structures.
                  </p>
                </div>
                <div className="flex-grow-1 flex-lg-grow-0" style={{ minWidth: '280px' }}>
                  <SearchBar
                    value={query}
                    onChange={setQuery}
                    onClear={() => setQuery('')}
                  />
                </div>
              </div>
              {schemaError ? (
                <div className="text-danger small mt-2">
                  Unable to load schema: {schemaError}
                </div>
              ) : schema ? (
                <div className="text-muted small mt-2">
                  Schema loaded for ontology {ontologyId}.
                </div>
              ) : null}
            </div>
          </div>
        </div>

        <div className="col-12 col-lg-4">
          <div className="card term-browser__panel shadow-sm">
            <div className="card-header bg-white fw-semibold">Ontology Tree</div>
            <div className="card-body">
              <TreeView
                data={filteredData}
                selectedId={selectedTerm?.id}
                expandedIds={expandedIds}
                loadingIds={loadingIds}
                onSelect={setSelectedTerm}
                onToggle={handleToggle}
              />
            </div>
          </div>
        </div>

        <div className="col-12 col-lg-8">
          <DetailsPanel
            term={selectedTerm}
            className="term-browser__panel shadow-sm"
          />
        </div>
      </div>
    </div>
  )
}

export default TermBrowser
