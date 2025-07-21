"""
`config` module: Configuration dataclasses for parsers

The configuration classes follow a hierarchy:
- Base*Config: Common options shared by all parsers
- WARC*Config: WARC-specific options (extends Base*Config)
- WARCGZ*Config: Gzip-specific options (extends Base*Config)
"""

from dataclasses import dataclass
from typing import Optional, List


@dataclass
class BaseCachingConfig:
    """
    Common caching configuration shared between WARCParser and WARCGZParser.

    This configuration controls what data is cached in memory during parsing.
    Caching data improves access speed and reduces I/O but increases memory usage.

    If caching is disabled, data may optionally be loaded from file on-demand;
    see `enable_lazy_loading_of_bytes`.

    Attributes:
        record_bytes: If True, cache the raw bytes of each WARC record.
        header_bytes: If True, cache the raw bytes of each WARC record header.
        parsed_headers: If True, cache the WARC header fields parsed into a dictionary.
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

    Adds options for handling unparsable lines encountered during parsing.
    Useful for inspecting malformed or corrupted WARC files.

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
    for handling gzip members that don't contain valid WARC records (useful for inspecting
    malformed or corrupted WARC.GZ files).

    Attributes:
        member_bytes: If True, cache the raw compressed bytes of each gzip member.
        member_uncompressed_bytes: If True, cache the decompressed bytes of each gzip member.
        non_warc_member_bytes: If True, cache bytes from gzip members that don't contain valid WARC records.
    """

    member_bytes: bool = False
    member_uncompressed_bytes: bool = False
    non_warc_member_bytes: bool = False


@dataclass
class BaseProcessorConfig:
    """
    Common processor configuration shared between WARCParser and WARCGZParser.

    This configuration controls what processors are applied during parsing, including
    filters, handlers, and callbacks.

    See "Filters, handlers, and callbacks" in README.md for details.

    Attributes:
        record_filters: List of functions to filter WARC records.
        record_handlers: List of functions to handle WARC records.
        parser_callbacks: List of functions to call when parsing is complete.
    """

    record_filters: Optional[List] = None
    record_handlers: Optional[List] = None
    parser_callbacks: Optional[List] = None


@dataclass
class WARCProcessorConfig(BaseProcessorConfig):
    """
    Processor configuration specific to WARCParser.

    Adds options for handling unparsable lines encountered during parsing.

    Attributes:
        unparsable_line_handlers: List of functions to handle unparsable lines.
    """

    unparsable_line_handlers: Optional[List] = None


@dataclass
class WARCGZProcessorConfig(BaseProcessorConfig):
    """
    Processor configuration specific to WARCGZParser.

    Adds options for handling gzip members (see warcbench.models.GzippedMember).

    Attributes:
        member_filters: List of functions to filter gzip members.
        member_handlers: List of functions to handle gzip members.
    """

    member_filters: Optional[List] = None
    member_handlers: Optional[List] = None


@dataclass
class BaseParsingConfig:
    """
    Common parsing configuration shared between WARCParser and WARCGZParser.

    Attributes:
        style: The parsing strategy to use.
        stop_after_nth: Stop parsing after the nth record/member.
        split_records: Whether to split records into headers and content blocks.
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
            - "delimiter": Parse by looking for WARC record delimiters
            - "content_length": Parse by using Content-Length headers
        parsing_chunk_size: Size of chunks to read when parsing in delimiter mode.
        check_content_lengths: Whether to validate content lengths in delimiter mode.
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

    Attributes:
        style: The parsing strategy to use. Allowed values:
            - "split_gzip_members": Split the file into individual gzip members
        decompress_and_parse_members: Whether to decompress and further parse members,
             or just locate the boundaries of members.
        decompression_style: The decompression strategy ("member" or "file").
            "member" means decompress each gzip member separately, one by one.
            "file" means decompress the whole file at once.
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
