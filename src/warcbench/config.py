"""
`config` module: Configuration dataclasses for parsers

The configuration classes follow a hierarchy:
- BaseCachingConfig: Common options shared by all parsers
- WARCCachingConfig: WARC-specific options (extends BaseCachingConfig)
- WARCGZCachingConfig: Gzip-specific options (extends BaseCachingConfig)
"""

from dataclasses import dataclass
from typing import Optional, List, Callable


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
