"""
`exceptions` module: Custom exceptions
"""


class AttributeNotInitializedError(Exception):
    """Custom exception raised when trying to access an uninitialized attribute."""

    pass


class DecompressionError(Exception):
    """Custom exception raised when trying to decompress a compressed WARC file."""

    pass


class DecodingException(Exception):
    """Custom exception raised when trying to decode an HTTP body block with Content-Encoding set in the header."""

    pass
