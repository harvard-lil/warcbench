"""
`exceptions` module: Custom exceptions
"""


class AttributeNotInitializedError(Exception):
    """Custom exception raised when trying to access an uninitialized attribute."""

    pass


class DecompressionError(Exception):
    """Custom exception raised when trying to decompress a compressed WARC file."""

    pass
