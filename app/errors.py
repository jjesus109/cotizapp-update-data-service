class DBConnectionError(Exception):
    """When could not connect to database"""


class ElementNotFoundError(Exception):
    """When element not found in DB"""


class InsertionError(Exception):
    """When there was a problem while inserting a DB"""


class MessageProcessingError(Exception):
    """When could not process data from message broker"""
