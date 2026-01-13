const GRAPHQL_ENDPOINT = new URL('/api/graphql', window.location.origin).toString()

const INTROSPECTION_QUERY = `query IntrospectionQuery {
  __schema {
    queryType { name }
    types {
      kind
      name
      fields {
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

function buildOntologyQueryFromSchema(schema, ontologyId) {
  return {
    query: `query PlaceholderOntologyQuery($ontologyId: ID) {\n  __typename\n}`,
    variables: { ontologyId },
  }
}

export { GRAPHQL_ENDPOINT, fetchGraphQL, fetchIntrospectionSchema, buildOntologyQueryFromSchema }
