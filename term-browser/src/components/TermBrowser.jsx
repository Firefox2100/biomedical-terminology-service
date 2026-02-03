import { useEffect, useMemo, useRef, useState } from 'react'
import SearchBar from './SearchBar.jsx'
import TreeView from './TreeView.jsx'
import DetailsPanel from './DetailsPanel.jsx'
import { ontologyConfig } from '../data/ontologyConfig.js'
import {
  buildAutoCompleteQuery,
  buildChildrenQuery,
  buildPathsToQuery,
  buildTermDetailQuery,
  fetchGraphQL,
  fetchIntrospectionSchema,
} from '../services/graphql.js'

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

function findNodeById(nodes, nodeId) {
  for (const node of nodes) {
    if (node.id === nodeId) {
      return node
    }
    if (node.children?.length) {
      const match = findNodeById(node.children, nodeId)
      if (match) {
        return match
      }
    }
  }
  return null
}

function getConceptId(node) {
  return node?.conceptId || node?.id || null
}

function TermBrowser({ ontologyId, rootConceptIds, returnPath }) {
  const [query, setQuery] = useState('')
  const [selectedTerm, setSelectedTerm] = useState(null)
  const [selectedDetails, setSelectedDetails] = useState(null)
  const [treeData, setTreeData] = useState([])
  const [expandedIds, setExpandedIds] = useState(() => new Set())
  const [loadingIds, setLoadingIds] = useState(() => new Set())
  const [schema, setSchema] = useState(null)
  const [schemaError, setSchemaError] = useState(null)
  const [treeError, setTreeError] = useState(null)
  const [detailError, setDetailError] = useState(null)
  const [detailLoading, setDetailLoading] = useState(false)
  const [searchResults, setSearchResults] = useState([])
  const [searchLoading, setSearchLoading] = useState(false)
  const [searchError, setSearchError] = useState(null)
  const [showSuggestions, setShowSuggestions] = useState(false)
  const detailCache = useRef(new Map())
  const searchRequestId = useRef(0)
  const searchSelectionRequestId = useRef(0)
  const treeDataRef = useRef(treeData)

  const filteredData = useMemo(() => treeData, [treeData])
  const resolvedRootIds = useMemo(() => {
    const configValue =
      ontologyConfig.rootConceptIdByOntologyId[ontologyId?.toLowerCase()]
    const baseIds =
      rootConceptIds?.length
        ? rootConceptIds
        : Array.isArray(configValue)
          ? configValue
          : configValue
            ? [configValue]
            : []
    return baseIds.filter(Boolean)
  }, [ontologyId, rootConceptIds])
  const fieldOverrides = ontologyConfig.queryFieldByOntologyId

  useEffect(() => {
    treeDataRef.current = treeData
  }, [treeData])

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

  useEffect(() => {
    setExpandedIds(new Set())
    setLoadingIds(new Set())
    setSelectedTerm(null)
    setSelectedDetails(null)
    setTreeError(null)
    setDetailError(null)
    setSearchResults([])
    setSearchError(null)
    setShowSuggestions(false)
    detailCache.current.clear()
  }, [ontologyId])

  useEffect(() => {
    if (!schema || !ontologyId) {
      return
    }

    if (!resolvedRootIds.length) {
      setTreeData([])
      return
    }

    const rootNodes = resolvedRootIds.map((rootId) => ({
      id: rootId,
      label: rootId,
      hasChildren: true,
    }))
    setTreeData(rootNodes)
  }, [schema, ontologyId, resolvedRootIds])

  useEffect(() => {
    if (!schema || !ontologyId || !resolvedRootIds.length) {
      return
    }

    let isMounted = true

    async function loadRootLabels() {
      try {
        const detailConfig = buildTermDetailQuery({
          schema,
          ontologyId,
          fieldOverrides,
        })

        if (!detailConfig) {
          return
        }

        const results = await Promise.all(
          resolvedRootIds.map(async (rootId) => {
            const data = await fetchGraphQL({
              query: detailConfig.query,
              variables: { conceptId: rootId },
            })
            const responseBlock = data?.[detailConfig.rootFieldName]
            const detail = responseBlock?.[detailConfig.conceptFieldName]?.data
            return { rootId, label: detail?.label }
          }),
        )

        if (!isMounted) {
          return
        }

        setTreeData((current) =>
          current.map((node) => {
            const match = results.find((item) => item.rootId === node.id)
            if (!match?.label) {
              return node
            }
            return { ...node, label: match.label }
          }),
        )
      } catch (error) {
        if (!isMounted) {
          return
        }
        setTreeError(error.message)
      }
    }

    loadRootLabels()

    return () => {
      isMounted = false
    }
  }, [schema, ontologyId, resolvedRootIds, fieldOverrides])

  useEffect(() => {
    const trimmedQuery = query.trim()
    if (!trimmedQuery) {
      setSearchResults([])
      setSearchError(null)
      setShowSuggestions(false)
      setSearchLoading(false)
      return
    }

    if (!schema || !ontologyId) {
      return
    }

    const currentRequest = searchRequestId.current + 1
    searchRequestId.current = currentRequest
    setSearchLoading(true)
    setSearchError(null)

    const timer = window.setTimeout(async () => {
      try {
        const queryConfig = buildAutoCompleteQuery({
          schema,
          ontologyId,
          fieldOverrides,
        })

        if (!queryConfig) {
          throw new Error('Unable to resolve auto-complete query fields.')
        }

        const result = await fetchGraphQL({
          query: queryConfig.query,
          variables: {
            search: trimmedQuery,
          },
        })

        if (searchRequestId.current !== currentRequest) {
          return
        }

        const items =
          result?.[queryConfig.rootFieldName]?.[queryConfig.autoCompleteFieldName]
            ?.data || []
        const mapped = items.map((item) => ({
          id: item.conceptId || item.id,
          label: item.label || item.conceptId || item.id,
          raw: item,
        }))
        setSearchResults(mapped)
        setShowSuggestions(true)
      } catch (error) {
        if (searchRequestId.current !== currentRequest) {
          return
        }
        setSearchError(error.message)
        setSearchResults([])
        setShowSuggestions(true)
      } finally {
        if (searchRequestId.current === currentRequest) {
          setSearchLoading(false)
        }
      }
    }, 2000)

    return () => {
      window.clearTimeout(timer)
    }
  }, [query, schema, ontologyId, fieldOverrides])

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

    if (!schema || !ontologyId) {
      return
    }

    if (!isExpanded && !node.children && node.hasChildren) {
      setLoadingIds((current) => new Set(current).add(node.id))
      setTreeError(null)

      try {
        const queryConfig = buildChildrenQuery({
          schema,
          ontologyId,
          fieldOverrides,
        })
        if (!queryConfig) {
          throw new Error('Unable to resolve ontology query fields.')
        }

        const result = await fetchGraphQL({
          query: queryConfig.query,
          variables: { conceptId: node.id },
        })

        const children =
          result?.[queryConfig.rootFieldName]?.[queryConfig.conceptFieldName]?.data
            ?.children || []

        const mappedChildren = (children || []).map((child) => ({
          id: child.conceptId || child.id,
          label: child.label || child.conceptId,
          hasChildren: true,
        }))

        setTreeData((current) =>
          updateNodeById(current, node.id, (currentNode) => ({
            ...currentNode,
            children: mappedChildren,
            hasChildren: mappedChildren.length > 0,
          })),
        )
      } catch (error) {
        setTreeError(error.message)
      } finally {
        setLoadingIds((current) => {
          const next = new Set(current)
          next.delete(node.id)
          return next
        })
      }
    }
  }

  const loadChildrenForNode = async (nodeId, queryConfig) => {
    const existing = findNodeById(treeDataRef.current, nodeId)
    if (Array.isArray(existing?.children)) {
      return existing.children
    }

    setLoadingIds((current) => new Set(current).add(nodeId))
    setTreeError(null)
    try {
      const result = await fetchGraphQL({
        query: queryConfig.query,
        variables: { conceptId: nodeId },
      })

      const children =
        result?.[queryConfig.rootFieldName]?.[queryConfig.conceptFieldName]?.data
          ?.children || []

      const mappedChildren = (children || []).map((child) => ({
        id: child.conceptId || child.id,
        label: child.label || child.conceptId,
        hasChildren: true,
      }))

      setTreeData((current) => {
        const next = updateNodeById(current, nodeId, (currentNode) => ({
          ...currentNode,
          children: mappedChildren,
          hasChildren: mappedChildren.length > 0,
        }))
        treeDataRef.current = next
        return next
      })

      return mappedChildren
    } finally {
      setLoadingIds((current) => {
        const next = new Set(current)
        next.delete(nodeId)
        return next
      })
    }
  }

  const ensureChildrenLoaded = async (nodeId, queryConfig) => {
    const node = findNodeById(treeDataRef.current, nodeId)
    if (!node || !node.hasChildren) {
      return []
    }
    if (Array.isArray(node.children)) {
      return node.children
    }
    return loadChildrenForNode(nodeId, queryConfig)
  }

  const expandTreeToConcept = async (conceptId) => {
    if (!schema || !ontologyId || !resolvedRootIds.length) {
      return null
    }

    const pathsQueryConfig = buildPathsToQuery({
      schema,
      ontologyId,
      fieldOverrides,
      maxDepth: 10,
    })
    if (!pathsQueryConfig) {
      return null
    }

    const childrenQueryConfig = buildChildrenQuery({
      schema,
      ontologyId,
      fieldOverrides,
    })
    if (!childrenQueryConfig) {
      throw new Error('Unable to resolve ontology query fields.')
    }

    const requestId = searchSelectionRequestId.current
    const roots = resolvedRootIds.filter(Boolean)
    const pathResponses = await Promise.all(
      roots.map(async (rootId) => {
        const result = await fetchGraphQL({
          query: pathsQueryConfig.query,
          variables: {
            conceptId: rootId,
            targetConceptId: conceptId,
            maxDepth: pathsQueryConfig.maxDepth,
          },
        })
        const data =
          result?.[pathsQueryConfig.rootFieldName]?.[
            pathsQueryConfig.conceptFieldName
          ]?.data
        return data?.pathsTo || []
      }),
    )

    if (searchSelectionRequestId.current !== requestId) {
      return null
    }

    const paths = pathResponses.flat().filter((path) => path?.nodes?.length)
    if (!paths.length) {
      return null
    }

    const bestPath = paths.reduce((best, current) =>
      (current.length || current.nodes.length) < (best.length || best.nodes.length)
        ? current
        : best,
    )

    const pathIds = bestPath.nodes.map((node) => getConceptId(node)).filter(Boolean)
    if (!pathIds.length) {
      return null
    }

    setExpandedIds((current) => {
      const next = new Set(current)
      pathIds.slice(0, -1).forEach((id) => next.add(id))
      return next
    })

    for (const id of pathIds.slice(0, -1)) {
      if (searchSelectionRequestId.current !== requestId) {
        return null
      }
      await loadChildrenForNode(id, childrenQueryConfig)
    }

    return findNodeById(treeDataRef.current, conceptId)
  }

  const handleSelect = async (node) => {
    setSelectedTerm(node)
    setSelectedDetails(null)
    setDetailError(null)

    if (!schema || !ontologyId) {
      return
    }

    if (detailCache.current.has(node.id)) {
      setSelectedDetails(detailCache.current.get(node.id))
      return
    }

    const detailConfig = buildTermDetailQuery({
      schema,
      ontologyId,
      fieldOverrides,
    })

    if (!detailConfig) {
      setDetailError('Unable to resolve ontology query fields.')
      return
    }

    setDetailLoading(true)
    try {
      const result = await fetchGraphQL({
        query: detailConfig.query,
        variables: { conceptId: node.id },
      })

      const responseBlock = result?.[detailConfig.rootFieldName]
      const data = responseBlock?.[detailConfig.conceptFieldName]?.data
      const resolved = data || null
      if (resolved) {
        detailCache.current.set(node.id, resolved)
      }
      setSelectedDetails(resolved)
    } catch (error) {
      setDetailError(error.message)
    } finally {
      setDetailLoading(false)
    }
  }

  const handleSearchSelection = async (item) => {
    const currentRequest = searchSelectionRequestId.current + 1
    searchSelectionRequestId.current = currentRequest

    setTreeError(null)
    const fallbackNode = { id: item.id, label: item.label }
    await handleSelect(fallbackNode)
    try {
      const match = await expandTreeToConcept(item.id)
      if (searchSelectionRequestId.current !== currentRequest) {
        return
      }
      if (match) {
        setSelectedTerm(match)
      }
    } catch (error) {
      if (searchSelectionRequestId.current !== currentRequest) {
        return
      }
      setTreeError(error.message)
    }
  }

  const handleReturn = () => {
    if (returnPath) {
      window.location.assign(returnPath)
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
                <div className="d-flex align-items-center gap-2 flex-grow-1 flex-lg-grow-0">
                  <div style={{ minWidth: '280px' }}>
                    <SearchBar
                      value={query}
                      onChange={(value) => {
                        setQuery(value)
                        setShowSuggestions(Boolean(value.trim()))
                      }}
                      onClear={() => {
                        setQuery('')
                        setSearchResults([])
                        setShowSuggestions(false)
                      }}
                      suggestions={searchResults}
                      onSelectSuggestion={(item) => {
                        setQuery(item.label)
                        setShowSuggestions(false)
                        handleSearchSelection(item)
                      }}
                      isLoading={searchLoading}
                      error={searchError}
                      showSuggestions={showSuggestions}
                    />
                  </div>
                  {returnPath ? (
                    <button
                      type="button"
                      className="btn btn-outline-secondary"
                      onClick={handleReturn}
                    >
                      Back
                    </button>
                  ) : null}
                </div>
              </div>
              {schemaError ? (
                <div className="text-danger small mt-2">
                  Unable to load schema: {schemaError}
                </div>
              ) : null}
            </div>
          </div>
        </div>

        <div className="col-12 col-lg-4">
          <div className="card term-browser__panel shadow-sm">
            <div className="card-header bg-white fw-semibold">Ontology Tree</div>
            <div className="card-body">
              {!ontologyId ? (
                <div className="text-muted">Select an ontology to begin.</div>
              ) : !resolvedRootIds.length ? (
                <div className="text-muted">
                  Set root concept ids via URL or config to load the tree.
                </div>
              ) : treeError ? (
                <div className="text-danger">Tree load failed: {treeError}</div>
              ) : null}
              <TreeView
                data={filteredData}
                selectedId={selectedTerm?.id}
                expandedIds={expandedIds}
                loadingIds={loadingIds}
                onSelect={handleSelect}
                onToggle={handleToggle}
              />
            </div>
          </div>
        </div>

        <div className="col-12 col-lg-8">
          <DetailsPanel
            term={selectedDetails || selectedTerm}
            isLoading={detailLoading}
            error={detailError}
            className="term-browser__panel shadow-sm"
          />
        </div>
      </div>
    </div>
  )
}

export default TermBrowser
