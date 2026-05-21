=====
Usage
=====

This document section explains how to use this service, including its features, query parameters, and potential applications.

The API provided by this service have multiple versions, corresponding to different tools intended to work with it. They can be broadly categorized into three groups:

* Cafe Variome V2 legacy support: This API version existed before the prototype of this service was built. As a result, it was designed for a different PHP service to provide similar features, and is not optimized for the current service architecture. It should not be used in any new applications.
* Cafe Variome V3 support: These API endpoints are designed for modern asynchronous web frameworks with better performance and scalability. They provide improved features and optimizations compared to the legacy V2 API. They are mostly POST endpoints, and the server collects all data before returning a response. If a system was originally designed to work with these endpoints, they can safely continue to do so, or consider migrating to the latest API version for better performance.
* Latest API for load balancer caching and streaming: This is the most recent API version, designed to work with load balancers and caching mechanisms for improved performance. These endpoints are all GET endpoints, allowing for better caching at the load balancer level. They also stream the response data as it generates, putting less load on server memory and improving response times for large datasets. If your application can work with streamed responses and proxy cache, it is recommended to use this latest API version.
* GraphQL API: All features provided by the REST API are also available through a GraphQL endpoint. This endpoint is not versioned and is subject to continuous improvements. It allows a fine-grain selective access to all data fields, making it suitable for applications that require specific data retrieval without over-fetching.
* MCP: The MCP API is a specialized API designed for usage with LLMs and agents. It provides a more structured and efficient way to interact with the service, particularly for applications that require complex queries or interactions. It is recommended for applications that need to leverage LLMs or agents for data processing and retrieval.

.. toctree::
    :maxdepth: 2
    :caption: Usage Topics:

    auto-complete
