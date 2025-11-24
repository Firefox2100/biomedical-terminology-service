class BtsError(Exception):
    """
    Base class for all Bioterms exceptions.
    """

    def __init__(self,
                 message: str = 'An error occurred in Bioterms',
                 status_code: int = 500
                 ):
        super().__init__(message)

        self.message = message
        self.status_code = status_code


class FilesNotFound(BtsError):
    """Raised when required data files are not found."""
    def __init__(self,
                 message: str = 'Required data files not found',
                 status_code: int = 404
                 ):
        super().__init__(message, status_code)


class IndexCreationError(BtsError):
    """Raised when there is an error creating an index in the database."""
    def __init__(self,
                 message: str = 'Error creating index in the database',
                 status_code: int = 500
                 ):
        super().__init__(message, status_code)


class VocabularyNotLoaded(BtsError):
    """Raised when a vocabulary is not loaded."""
    def __init__(self,
                 message: str = 'Vocabulary not loaded',
                 status_code: int = 400
                 ):
        super().__init__(message, status_code)
