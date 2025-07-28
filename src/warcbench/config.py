"""
`config` module: Configuration dataclasses for parsers

The configuration classes follow a hierarchy:
- Base*Config: Common options shared by all parsers
- WARC*Config: WARC-specific options (extends Base*Config)
- WARCGZ*Config: Gzipped WARC-specific options (extends Base*Config)
"""

from dataclasses import dataclass
from typing import Optional, List, Callable, TYPE_CHECKING, Union

if TYPE_CHECKING:
    from warcbench.models import (
        Record,
        UnparsableLine,
        GzippedMember,
    )  # pragma: no cover
    from warcbench.parsers.warc import BaseParser as WARCBaseParser  # pragma: no cover
    from warcbench.parsers.gzipped_warc import (
        BaseParser as WARCGZBaseParser,
    )  # pragma: no cover


#
# Parsing
#


@dataclass
class BaseParsingConfig:
    """
    Common parsing configuration shared between WARCParser and WARCGZParser.

    Attributes:
        style: The parsing strategy to use. Specific values depend on the parser type.
        stop_after_nth: Stop parsing after the nth record/member.
        split_records: Whether to split records into headers and content blocks,
            or forgo further parsing after identifying record boundaries.
    """

    style: Optional[str] = None
    stop_after_nth: Optional[int] = None
    split_records: bool = True


@dataclass
class WARCParsingConfig(BaseParsingConfig):
    """
    Parsing configuration specific to WARCParser.

    Attributes:
        style: The parsing strategy to use. Allowed values:
            - "delimiter": Parse by looking for WARC record delimiters (CRLF + CRLF).
                May be more robust for malformed files, but slower.
            - "content_length": Parse by using WARC record Content-Length headers to
                determine record boundaries. Faster, but requires well-formed WARC files.
        parsing_chunk_size: Size of chunks to read when parsing in delimiter mode.
        check_content_lengths: Whether to validate WARC record Content-Length headers.
            When True, compares the actual content length with the Content-Length header.
            Only meaningful when style="delimiter".
    """

    style: str = "content_length"
    parsing_chunk_size: int = 1024
    check_content_lengths: bool = False

    def __post_init__(self):
        if self.check_content_lengths and self.style == "content_length":
            raise ValueError(
                "Checking content lengths is only meaningful when parsing in delimiter mode."
            )


@dataclass
class WARCGZParsingConfig(BaseParsingConfig):
    """
    Parsing configuration specific to WARCGZParser.

    Controls how WARC.GZ files, which may contain multiple gzip members, are parsed
    and decompressed. See warcbench.models.GzippedMember for details about members.

    Attributes:
        style: The parsing strategy to use. At present, only one supported value:
            - "split_gzip_members"

        decompress_and_parse_members: Whether to decompress and further parse members,
            or just locate the boundaries of members.
            When True, each member is decompressed and parsed as WARC content.
            When False, only member boundaries are identified.

        decompression_style: The decompression strategy.
            - "file": Decompress the whole file at once (may use more space on disk)
            - "member": Decompress each gzip member sequentially, one by one
               (simpler strategy, but potentially much slower, especially for large files).

        decompress_chunk_size: Size of chunks to use during decompression.
    """

    style: str = "split_gzip_members"
    decompress_and_parse_members: bool = True
    decompression_style: str = "file"
    decompress_chunk_size: int = 1024

    def __post_init__(self):
        if (
            not self.decompress_and_parse_members
            and self.decompression_style != "member"
        ):
            raise ValueError(
                "Decompressing records can only be disabled when decompression style is set to 'member'."
            )


#
# Processing
#


@dataclass
class BaseProcessorConfig:
    """
    Common processor configuration shared between WARCParser and WARCGZParser.

    Processors are applied in the following order:
    1. Filters: Determine which records to keep or discard
    2. Handlers: Process each record (e.g., extract data, transform content)
    3. Callbacks: Final processing when parsing completes

    See "Filters, handlers, and callbacks" in README.md for detailed examples.

    Attributes:
        record_filters: List of filter functions. Each function takes a Record object
            and returns True to keep the record, False to discard it.
        record_handlers: List of handler functions. Each function takes a Record object
            and performs some processing (e.g., data extraction, transformation).
        parser_callbacks: List of callback functions. Each function takes the parser
            object and is called when parsing completes.
    """

    record_filters: Optional[List[Callable[["Record"], bool]]] = None
    record_handlers: Optional[List[Callable[["Record"], None]]] = None
    parser_callbacks: Optional[
        List[Callable[[Union["WARCBaseParser", "WARCGZBaseParser"]], None]]
    ] = None


@dataclass
class WARCProcessorConfig(BaseProcessorConfig):
    """
    Processor configuration specific to WARCParser.

    Adds an option for handling unparsable lines encountered during parsing.
    (see warcbench.models.UnparsableLine)

    Attributes:
        unparsable_line_handlers: List of handler functions. Each function takes an
            UnparsableLine object and performs some processing (e.g. logging,
            analysis).
    """

    unparsable_line_handlers: Optional[List[Callable[["UnparsableLine"], None]]] = None


@dataclass
class WARCGZProcessorConfig(BaseProcessorConfig):
    """
    Processor configuration specific to WARCGZParser.

    Adds options for handling gzip members (see warcbench.models.GzippedMember).

    Attributes:
        member_filters: List of functions to filter gzip members.
            Each function takes a GzippedMember object and returns True to keep
            the member, False to skip it.
        member_handlers: List of functions to handle gzip members.
            Each function takes a GzippedMember object and performs some
            processing (e.g., metadata extraction, validation).
    """

    member_filters: Optional[List[Callable[["GzippedMember"], bool]]] = None
    member_handlers: Optional[List[Callable[["GzippedMember"], None]]] = None


#
# Caching
#


@dataclass
class BaseCachingConfig:
    """
    Common caching configuration shared between WARCParser and WARCGZParser.

    Controls what data is cached in memory during parsing.
    Caching data improves access speed and reduces I/O but increases memory usage.

    Consider enabling caching when accessing data in filters and handlers,
    when forwarding data to upstream code, or when debugging parsing issues.

    (Even if caching is disabled, data may optionally be loaded from files on-demand:
    see `enable_lazy_loading_of_bytes` in the parser constructor.)

    Attributes:
        record_bytes: If True, cache the raw bytes of each WARC record.
        header_bytes: If True, cache the raw bytes of each WARC record header.
        parsed_headers: If True, cache the WARC header fields parsed into a dictionary.
            Provides easy access to header fields like Content-Length, WARC-Type, etc.
            without the need to repeatedly parse raw header bytes.
        content_block_bytes: If True, cache the raw bytes of each WARC record content block.
    """

    record_bytes: bool = False
    header_bytes: bool = False
    parsed_headers: bool = False
    content_block_bytes: bool = False


@dataclass
class WARCCachingConfig(BaseCachingConfig):
    """
    Caching configuration specific to WARCParser.

    Adds options for unparsable lines (see warcbench.models.UnparsableLine) encountered during parsing.

    Attributes:
        unparsable_lines: If True, collect unparsable lines as UnparsableLine objects.
        unparsable_line_bytes: If True, cache the raw bytes of unparsable lines.
    """

    unparsable_lines: bool = False
    unparsable_line_bytes: bool = False


@dataclass
class WARCGZCachingConfig(BaseCachingConfig):
    """
    Caching configuration specific to WARCGZParser.

    Adds options for caching gzip members (see warcbench.models.GzippedMember) and
    for handling gzip members that don't contain valid WARC records.

    Attributes:
        member_bytes: If True, cache the raw compressed bytes of each gzip member.
        member_uncompressed_bytes: If True, cache the decompressed bytes of each gzip member.
        non_warc_member_bytes: If True, cache bytes from gzip members that don't contain valid WARC records.
    """

    member_bytes: bool = False
    member_uncompressed_bytes: bool = False
    non_warc_member_bytes: bool = False
