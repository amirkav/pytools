class Error(Exception):
    """Generic DatabaseAdapter error"""


class ResponseError(Error):
    """Unusable database response"""
