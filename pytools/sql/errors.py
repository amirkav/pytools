class SQLError(Exception):
    """Generic SQL database error."""


class NoDatabaseOfSuchTypeError(SQLError):
    """Project has no database of requested type."""
