=======================
Auto-Completion Service
=======================

This service provides dedicated auto-completion feature for usage with interactive UI components. The work flow is:

* In the UI, a user may input a partial string for a concept.
* The UI sends the partial string to this service. These endpoints are designed to work with frontend, focusing on fast responses, and has permissive CORS settings.
* This service returns a list of concepts that contains the provided partial string in the concept ID, label, synonym or definition.

Note that the auto-completion search uses exact match, and no fuzzy match or semantic search is implemented. If there is a typo in the string, there may be no results returned at all.
