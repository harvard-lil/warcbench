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


class SplitRecordsRequiredError(ValueError):
    """Custom exception raised when a method requires split_records=True parsing."""

    def __init__(self, method_name: str, requires: str = "parsed content blocks"):
        message = (
            f"{method_name}() requires {requires}. "
            "Parse with split_records=True to access headers and content blocks separately."
        )
        super().__init__(message)
