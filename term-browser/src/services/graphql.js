const GRAPHQL_ENDPOINT =
  import.meta.env.VITE_GRAPHQL_ENDPOINT ||
  new URL('/api/graphql/', window.location.origin).toString()

const INTROSPECTION_QUERY = `query IntrospectionQuery {
  __schema {
    queryType { name }
    types {
      kind
      name
      fields {
        name
        args {
          name
          type {
            kind
            name
            ofType {
              kind
              name
              ofType {
                kind
                name
              }
            }
          }
        }
        type {
          kind
          name
          ofType {
            kind
            name
            ofType {
              kind
              name
            }
          }
        }
      }
    }
  }
}`

async function fetchGraphQL({ query, variables }) {
  const response = await fetch(GRAPHQL_ENDPOINT, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({ query, variables }),
  })

  if (!response.ok) {
    throw new Error(`GraphQL request failed: ${response.status}`)
  }

  const payload = await response.json()
  if (payload.errors?.length) {
    const message = payload.errors.map((error) => error.message).join('; ')
    throw new Error(message)
  }

  return payload.data
}

async function fetchIntrospectionSchema() {
  return fetchGraphQL({ query: INTROSPECTION_QUERY })
}

function unwrapType(type) {
  if (!type) {
    return null
  }
  let current = type
  while (current.ofType) {
    current = current.ofType
  }
  return current
}

function getTypeSignature(type) {
  if (!type) {
    return 'String'
  }
  if (type.kind === 'NON_NULL') {
    return `${getTypeSignature(type.ofType)}!`
  }
  if (type.kind === 'LIST') {
    return `[${getTypeSignature(type.ofType)}]`
  }
  return type.name || 'String'
}

function getTypeMap(schema) {
  const map = new Map()
  schema?.types?.forEach((type) => {
    if (type?.name) {
      map.set(type.name, type)
    }
  })
  return map
}

function getQueryField(schema, ontologyId, fieldOverrides = {}) {
  if (!schema?.queryType?.name || !ontologyId) {
    return null
  }
  const typeMap = getTypeMap(schema)
  const queryType = typeMap.get(schema.queryType.name)
  if (!queryType?.fields?.length) {
    return null
  }

  const normalizedId = ontologyId.toLowerCase()
  const override = fieldOverrides[normalizedId]
  const desiredName = override || normalizedId

  return queryType.fields.find((field) => field.name.toLowerCase() === desiredName) || null
}

function getQueryFieldType(schema, field) {
  if (!schema || !field?.type) {
    return null
  }
  const typeMap = getTypeMap(schema)
  const namedType = unwrapType(field.type)
  return namedType?.name ? typeMap.get(namedType.name) : null
}

function getConceptField(queryType) {
  if (!queryType?.fields?.length) {
    return null
  }
  return queryType.fields.find((field) => field.name.toLowerCase().endsWith('concept')) || null
}

function getAutoCompleteField(queryType) {
  if (!queryType?.fields?.length) {
    return null
  }
  return queryType.fields.find((field) => field.name.toLowerCase() === 'autocomplete') || null
}

function getArgumentName(field, fallbackName) {
  if (!field?.args?.length) {
    return fallbackName
  }
  const conceptArg = field.args.find((arg) => arg.name.toLowerCase() === 'conceptid')
  return conceptArg?.name || field.args[0].name || fallbackName
}

function getSearchArgumentName(field, fallbackName) {
  if (!field?.args?.length) {
    return fallbackName
  }
  const preferredNames = ['query', 'search', 'text', 'term']
  const match = field.args.find((arg) =>
    preferredNames.includes(arg.name.toLowerCase()),
  )
  return match?.name || field.args[0].name || fallbackName
}

function getArgumentType(field, argName) {
  if (!field?.args?.length) {
    return null
  }
  const arg = field.args.find((argument) => argument.name === argName)
  return arg?.type || null
}

function getConceptResponseType(schema, conceptField) {
  const typeMap = getTypeMap(schema)
  const responseType = unwrapType(conceptField?.type)
  return responseType?.name ? typeMap.get(responseType.name) : null
}

function getConceptDataType(schema, responseType) {
  if (!responseType?.fields?.length) {
    return null
  }
  const typeMap = getTypeMap(schema)
  const dataField = responseType.fields.find((field) => field.name === 'data')
  const dataType = unwrapType(dataField?.type)
  return dataType?.name ? typeMap.get(dataType.name) : null
}

function isScalarLike(type) {
  return type?.kind === 'SCALAR' || type?.kind === 'ENUM'
}

function buildDetailSelection(schema, conceptType) {
  if (!conceptType?.fields?.length) {
    return 'conceptId label'
  }

  const typeMap = getTypeMap(schema)
  const selections = []

  conceptType.fields.forEach((field) => {
    const namedType = unwrapType(field.type)
    const resolvedType = namedType?.name ? typeMap.get(namedType.name) : null

    if (isScalarLike(namedType) || isScalarLike(resolvedType)) {
      selections.push(field.name)
      return
    }

    const isList = field.type.kind === 'LIST' || field.type.ofType?.kind === 'LIST'
    if (isList && isScalarLike(namedType)) {
      selections.push(field.name)
      return
    }

    if (resolvedType?.fields?.some((innerField) => innerField.name === 'conceptId')) {
      selections.push(`${field.name} { conceptId label }`)
    }
  })

  if (!selections.includes('conceptId')) {
    selections.unshift('conceptId')
  }
  if (!selections.includes('label')) {
    selections.unshift('label')
  }

  return selections.join('\n    ')
}

function buildChildrenQuery({ schema, ontologyId, fieldOverrides }) {
  const rootField = getQueryField(schema, ontologyId, fieldOverrides)
  const queryType = getQueryFieldType(schema, rootField)
  const conceptField = getConceptField(queryType)
  const conceptArg = getArgumentName(conceptField, 'conceptId')
  const conceptArgType = getTypeSignature(getArgumentType(conceptField, conceptArg))
  const responseType = getConceptResponseType(schema, conceptField)
  const conceptType = getConceptDataType(schema, responseType)
  const conceptTypeName = conceptType?.name || 'Concept'

  if (!rootField || !conceptField) {
    return null
  }

  return {
    query: `query TermChildren($conceptId: ${conceptArgType}) {\n  ${rootField.name} {\n    ${conceptField.name}(${conceptArg}: $conceptId) {\n      data {\n        children {\n          conceptId\n          label\n          __typename\n        }\n        __typename\n      }\n      error {\n        message\n        code\n      }\n      __typename\n    }\n    __typename\n  }\n  __typename\n}`,
    variables: { conceptId: null },
    rootFieldName: rootField.name,
    conceptFieldName: conceptField.name,
    conceptTypeName,
  }
}

function buildTermDetailQuery({ schema, ontologyId, fieldOverrides }) {
  const rootField = getQueryField(schema, ontologyId, fieldOverrides)
  const queryType = getQueryFieldType(schema, rootField)
  const conceptField = getConceptField(queryType)
  const conceptArg = getArgumentName(conceptField, 'conceptId')
  const conceptArgType = getTypeSignature(getArgumentType(conceptField, conceptArg))
  const responseType = getConceptResponseType(schema, conceptField)
  const conceptType = getConceptDataType(schema, responseType)
  const selection = buildDetailSelection(schema, conceptType)

  if (!rootField || !conceptField) {
    return null
  }

  return {
    query: `query TermDetail($conceptId: ${conceptArgType}) {\n  ${rootField.name} {\n    ${conceptField.name}(${conceptArg}: $conceptId) {\n      data {\n        ${selection}\n      }\n      error {\n        message\n        code\n      }\n    }\n  }\n}`,
    variables: { conceptId: null },
    rootFieldName: rootField.name,
    conceptFieldName: conceptField.name,
  }
}

function buildAutoCompleteQuery({ schema, ontologyId, fieldOverrides }) {
  const rootField = getQueryField(schema, ontologyId, fieldOverrides)
  const queryType = getQueryFieldType(schema, rootField)
  const autoCompleteField = getAutoCompleteField(queryType)
  const searchArg = getSearchArgumentName(autoCompleteField, 'query')
  const searchArgType = getTypeSignature(getArgumentType(autoCompleteField, searchArg))

  if (!rootField || !autoCompleteField) {
    return null
  }

  return {
    query: `query TermAutoComplete($search: ${searchArgType}) {\n  ${rootField.name} {\n    ${autoCompleteField.name}(${searchArg}: $search) {\n      data {\n        conceptId\n        label\n        __typename\n      }\n      error {\n        message\n        code\n      }\n    }\n  }\n}`,
    variables: { search: null },
    rootFieldName: rootField.name,
    autoCompleteFieldName: autoCompleteField.name,
    searchArgName: searchArg,
  }
}

function buildParentsSelection(depth) {
  if (depth <= 0) {
    return 'conceptId\n        label'
  }
  return `conceptId\n        label\n        parents {\n        ${buildParentsSelection(depth - 1)}\n      }`
}

function buildParentsQuery({ schema, ontologyId, fieldOverrides, depth = 2 }) {
  const rootField = getQueryField(schema, ontologyId, fieldOverrides)
  const queryType = getQueryFieldType(schema, rootField)
  const conceptField = getConceptField(queryType)
  const conceptArg = getArgumentName(conceptField, 'conceptId')
  const conceptArgType = getTypeSignature(getArgumentType(conceptField, conceptArg))
  const responseType = getConceptResponseType(schema, conceptField)
  const conceptType = getConceptDataType(schema, responseType)

  if (!rootField || !conceptField) {
    return null
  }

  const hasParents = conceptType?.fields?.some((field) => field.name === 'parents')
  if (!hasParents) {
    return null
  }

  const selection = buildParentsSelection(depth)

  return {
    query: `query TermParents($conceptId: ${conceptArgType}) {\n  ${rootField.name} {\n    ${conceptField.name}(${conceptArg}: $conceptId) {\n      data {\n        ${selection}\n      }\n      error {\n        message\n        code\n      }\n    }\n  }\n}`,
    variables: { conceptId: null },
    rootFieldName: rootField.name,
    conceptFieldName: conceptField.name,
  }
}

function buildChildrenSelection(depth) {
  if (depth <= 0) {
    return 'conceptId\n        label'
  }
  return `conceptId\n        label\n        children {\n        ${buildChildrenSelection(depth - 1)}\n      }`
}

function buildChildrenPathQuery({ schema, ontologyId, fieldOverrides, depth = 2 }) {
  const rootField = getQueryField(schema, ontologyId, fieldOverrides)
  const queryType = getQueryFieldType(schema, rootField)
  const conceptField = getConceptField(queryType)
  const conceptArg = getArgumentName(conceptField, 'conceptId')
  const conceptArgType = getTypeSignature(getArgumentType(conceptField, conceptArg))
  const responseType = getConceptResponseType(schema, conceptField)
  const conceptType = getConceptDataType(schema, responseType)

  if (!rootField || !conceptField) {
    return null
  }

  const hasChildren = conceptType?.fields?.some((field) => field.name === 'children')
  if (!hasChildren) {
    return null
  }

  const selection = buildChildrenSelection(depth)

  return {
    query: `query TermChildrenPath($conceptId: ${conceptArgType}) {\n  ${rootField.name} {\n    ${conceptField.name}(${conceptArg}: $conceptId) {\n      data {\n        ${selection}\n      }\n      error {\n        message\n        code\n      }\n    }\n  }\n}`,
    variables: { conceptId: null },
    rootFieldName: rootField.name,
    conceptFieldName: conceptField.name,
  }
}

function getFieldByName(type, name) {
  if (!type?.fields?.length) {
    return null
  }
  return type.fields.find((field) => field.name.toLowerCase() === name.toLowerCase())
}

function getArgNameInsensitive(field, desiredName, fallbackName) {
  if (!field?.args?.length) {
    return fallbackName
  }
  const match = field.args.find(
    (arg) => arg.name.toLowerCase() === desiredName.toLowerCase(),
  )
  return match?.name || fallbackName
}

function buildPathsToQuery({ schema, ontologyId, fieldOverrides, maxDepth = 10 }) {
  const rootField = getQueryField(schema, ontologyId, fieldOverrides)
  const queryType = getQueryFieldType(schema, rootField)
  const conceptField = getConceptField(queryType)
  const conceptArg = getArgumentName(conceptField, 'conceptId')
  const conceptArgType = getTypeSignature(getArgumentType(conceptField, conceptArg))
  const responseType = getConceptResponseType(schema, conceptField)
  const conceptType = getConceptDataType(schema, responseType)

  if (!rootField || !conceptField || !conceptType) {
    return null
  }

  const pathsField = getFieldByName(conceptType, 'pathsTo')
  if (!pathsField) {
    return null
  }

  const targetPrefixArg = getArgNameInsensitive(pathsField, 'targetPrefix', null)
  const relationshipArg = getArgNameInsensitive(pathsField, 'relationship', null)
  const directionArg = getArgNameInsensitive(pathsField, 'direction', null)
  const maxDepthArg = getArgNameInsensitive(pathsField, 'maxDepth', null)
  const targetConceptIdArg = getArgNameInsensitive(
    pathsField,
    'targetConceptId',
    null,
  )

  const args = []
  if (targetPrefixArg) {
    args.push(`${targetPrefixArg}: ${ontologyId.toLowerCase()}`)
  }
  if (relationshipArg) {
    args.push(`${relationshipArg}: is_a`)
  }
  if (directionArg) {
    args.push(`${directionArg}: reverse`)
  }
  if (maxDepthArg) {
    args.push(`${maxDepthArg}: $maxDepth`)
  }
  if (targetConceptIdArg) {
    args.push(`${targetConceptIdArg}: $targetConceptId`)
  }

  const argsClause = args.length ? `(${args.join(', ')})` : ''

  return {
    query: `query TermPaths($conceptId: ${conceptArgType}, $targetConceptId: ${conceptArgType}, $maxDepth: Int) {\n  ${rootField.name} {\n    ${conceptField.name}(${conceptArg}: $conceptId) {\n      data {\n        pathsTo${argsClause} {\n          length\n          nodes {\n            prefix\n            conceptId\n            label\n          }\n        }\n      }\n      error {\n        message\n        code\n      }\n    }\n  }\n}`,
    variables: { conceptId: null, targetConceptId: null, maxDepth },
    rootFieldName: rootField.name,
    conceptFieldName: conceptField.name,
    maxDepth,
  }
}

export {
  GRAPHQL_ENDPOINT,
  fetchGraphQL,
  fetchIntrospectionSchema,
  buildChildrenQuery,
  buildAutoCompleteQuery,
  buildTermDetailQuery,
  buildParentsQuery,
  buildChildrenPathQuery,
  buildPathsToQuery,
  getQueryField,
}
