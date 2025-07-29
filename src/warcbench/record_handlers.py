"""
`record_handlers` module: Functions that return helper functions that take a Record and return None
"""

from typing import cast, Callable

from warcbench.models import Record


def get_record_offsets(
    split: bool = False,
    append_to: list[tuple[int, int] | tuple[int, int, int, int]] | None = None,
    print_each: bool = True,
) -> Callable[[Record], None]:
    """
    A handler that extracts and optionally prints byte offsets of WARC records.

    Args:
        split: If True, extract separate offsets for headers and content blocks.
            If False, extract offsets for complete records.
        append_to: Optional list to append offset tuples to.
        print_each: If True, print offset information for each record.

    Returns:
        Callable[[Record], None]: A handler function that can be passed to
        WARCParser or WARCGZParser processors.

    Example:
        ```python
        offsets = []
        handler = get_record_offsets(split=True, append_to=offsets, print_each=False)
        parser = WARCParser(file, processors=WARCProcessorConfig(record_handlers=[handler]))
        ```
    """

    def f(record: Record) -> None:
        offsets: tuple[int, int] | tuple[int, int, int, int]
        if split:
            offsets = (
                record.header.start,  # type: ignore[union-attr]
                record.header.end,  # type: ignore[union-attr]
                record.content_block.start,  # type: ignore[union-attr]
                record.content_block.end,  # type: ignore[union-attr]
            )
        else:
            offsets = (record.start, record.end)

        if append_to is not None:
            append_to.append(offsets)

        if print_each:
            if split:
                split_offsets = cast(tuple[int, int, int, int], offsets)
                print(f"Record bytes {split_offsets[0]}-{split_offsets[3]}")
                print(f"Header bytes {split_offsets[0]}-{split_offsets[1]}")
                print(f"Content bytes {split_offsets[2]}-{split_offsets[3]}")
            else:
                print(f"Record bytes {offsets[0]}-{offsets[1]}")
            print()

    return f


def get_record_headers(
    decode_utf8: bool = True,
    append_to: list[str | bytes | None] | None = None,
    print_each: bool = True,
) -> Callable[[Record], None]:
    """
    A handler that extracts and optionally prints WARC record headers.

    Args:
        decode_utf8: If True, decode header bytes to UTF-8 strings.
        append_to: Optional list to append header data to.
        print_each: If True, print header content for each record.

    Returns:
        Callable[[Record], None]: A handler function that can be passed to
        WARCParser or WARCGZParser processors.

    Example:
        ```python
        headers = []
        handler = get_record_headers(append_to=headers, print_each=False)
        parser = WARCParser(file, processors=WARCProcessorConfig(record_handlers=[handler]))
        ```
    """

    def f(record: Record) -> None:
        if record.header:
            data = record.header.bytes
            if append_to is not None:
                header: str | bytes
                if decode_utf8:
                    header = data.decode("utf-8", errors="replace")
                else:
                    header = data
                append_to.append(header)

            if print_each:
                for header_line in data.split(b"\r\n"):
                    line: str | bytes
                    if decode_utf8:
                        line = header_line.decode("utf-8", errors="replace")
                    else:
                        line = header_line
                    if line:
                        print(line)
                print()
        else:
            if append_to is not None:
                append_to.append(None)

            if print_each:
                print("Record header not parsed.")
                print()

    return f


def get_record_content(
    append_to: list[bytes | None] | None = None, print_each: bool = False
) -> Callable[[Record], None]:
    """
    A handler that extracts and optionally prints WARC record content blocks.

    Args:
        append_to: Optional list to append content block bytes to.
        print_each: If True, print content for each record.

    Returns:
        Callable[[Record], None]: A handler function that can be passed to
        WARCParser or WARCGZParser processors.

    Example:
        ```python
        content_blocks = []
        handler = get_record_content(append_to=content_blocks, print_each=False)
        parser = WARCParser(file, processors=WARCProcessorConfig(record_handlers=[handler]))
        ```
    """

    def f(record: Record) -> None:
        if record.content_block:
            data = record.content_block.bytes
            if append_to is not None:
                append_to.append(data)

            if print_each:
                print(data)
                print()
        else:
            if append_to is not None:
                append_to.append(None)

            if print_each:
                print("Record content not parsed.")
                print()

    return f


def get_record_http_headers(
    decode: str = "ascii",
    append_to: list[str | None] | None = None,
    print_each: bool = True,
) -> Callable[[Record], None]:
    """
    A handler that extracts and optionally prints HTTP headers from WARC records.

    Only works with WARC records that contain HTTP request/response data.

    Args:
        decode: Encoding to use when decoding HTTP headers ("ascii", "utf-8", etc.).
        append_to: Optional list to append HTTP header data to.
        print_each: If True, print HTTP headers for each applicable record.

    Returns:
        Callable[[Record], None]: A handler function that can be passed to
        WARCParser or WARCGZParser processors.

    Example:
        ```python
        http_headers = []
        handler = get_record_http_headers(append_to=http_headers, print_each=False)
        parser = WARCParser(file, processors=WARCProcessorConfig(record_handlers=[handler]))
        ```
    """

    def f(record: Record) -> None:
        header_block = record.get_http_header_block()
        if header_block:
            if append_to is not None:
                if decode:
                    headers = header_block.decode(decode)
                else:
                    headers = record.header  # type: ignore[assignment]
                append_to.append(headers)

            if print_each:
                for header_line in header_block.split(b"\r\n"):
                    if decode:
                        line = header_line.decode(decode)
                    else:
                        line = header_line  # type: ignore[assignment]
                    if line:
                        print(line)
                print()
        else:
            if append_to is not None:
                append_to.append(None)

            if print_each:
                if record.content_block:
                    print("No HTTP headers parsed.")
                else:
                    print("Record content not parsed.")
                print()

    return f


def get_record_http_body(
    append_to: list[bytes | None] | None = None, print_each: bool = False
) -> Callable[[Record], None]:
    """
    A handler that extracts and optionally prints HTTP body content from WARC records.

    Only works with WARC records that contain HTTP request/response data.

    Args:
        append_to: Optional list to append HTTP body bytes to.
        print_each: If True, print HTTP body content for each applicable record.

    Returns:
        Callable[[Record], None]: A handler function that can be passed to
        WARCParser or WARCGZParser processors.

    Example:
        ```python
        http_bodies = []
        handler = get_record_http_body(append_to=http_bodies, print_each=False)
        parser = WARCParser(file, processors=WARCProcessorConfig(record_handlers=[handler]))
        ```
    """

    def f(record: Record) -> None:
        body = record.get_http_body_block()
        if body:
            if append_to is not None:
                append_to.append(body)

            if print_each:
                print(body)
                print()
        else:
            if append_to is not None:
                append_to.append(None)

            if print_each:
                if record.content_block:
                    print("No HTTP body parsed.")
                else:
                    print("Record content not parsed.")
                print()

    return f


def print_separator(separator: str = "-" * 40) -> Callable[[Record], None]:
    """
    A utility handler that prints a separator line between records.

    Useful for creating readable output when processing multiple records with print_each=True.

    Args:
        separator: The separator string to print (default: 40 dashes).

    Returns:
        Callable[[Record], None]: A handler function that can be passed to
        WARCParser or WARCGZParser processors.

    Example:
        ```python
        handlers = [
            get_record_headers(print_each=True),
            print_separator(),
            get_record_content(print_each=True),
            print_separator("=" * 50)
        ]
        parser = WARCParser(file, processors=WARCProcessorConfig(record_handlers=handlers))
        ```
    """

    def f(record: Record) -> None:
        print(separator)

    return f
