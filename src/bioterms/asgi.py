"""
ASGI server application for the BioMedical Terminology Service project.

For Python ASGI servers, invoke this module and the application object.
"""

from bioterms.app import create_app


application = create_app()
