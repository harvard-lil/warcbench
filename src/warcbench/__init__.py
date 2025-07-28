from warcbench.parsers import (
    DelimiterWARCParser,
    ContentLengthWARCParser,
    GzippedWARCMemberParser,
    GzippedWARCDecompressingParser,
)
from warcbench.config import (
    WARCParsingConfig,
    WARCGZParsingConfig,
    WARCProcessorConfig,
    WARCGZProcessorConfig,
    WARCCachingConfig,
    WARCGZCachingConfig,
)
from typing import Optional, Union, List, Tuple, Iterator, Any, TYPE_CHECKING, Dict
from io import BufferedReader

if TYPE_CHECKING:
    from warcbench.models import Record, UnparsableLine, GzippedMember


class WARCParser:
    """
    A parser for WARC (Web ARChive) files.

    This is the main interface for parsing WARC files. It supports multiple parsing
    strategies and provides access to records, warnings, and error information.

    Args:
        file_handle: A file-like object opened in binary mode
        enable_lazy_loading_of_bytes: Whether to enable on-demand byte access (default: True).
            When True, attaches file handles to parsed objects so bytes can be read
            on-demand when accessed. When False, bytes are only accessible if explicitly
            cached via the cache configuration. Independent of cache settings.
        parsing_options: Configuration for parsing behavior
        processors: Configuration for filters, handlers, and callbacks
        cache: Configuration for what data to cache in memory during parsing.
            Controls immediate caching of bytes, parsed headers, etc. Independent of
            enable_lazy_loading_of_bytes. When both are enabled, you get both immediate
            caching AND on-demand access capabilities.
    """

    def __init__(
        self,
        file_handle: BufferedReader,
        enable_lazy_loading_of_bytes: bool = True,
        parsing_options: Optional[WARCParsingConfig] = None,
        processors: Optional[WARCProcessorConfig] = None,
        cache: Optional[WARCCachingConfig] = None,
    ):
        # Set up default config
        if parsing_options is None:
            parsing_options = WARCParsingConfig()
        if cache is None:
            cache = WARCCachingConfig()
        if processors is None:
            processors = WARCProcessorConfig()

        # Initialize the appropriate parser
        self._parser: Union[DelimiterWARCParser, ContentLengthWARCParser]
        match parsing_options.style:
            case "delimiter":
                self._parser = DelimiterWARCParser(
                    file_handle=file_handle,
                    enable_lazy_loading_of_bytes=enable_lazy_loading_of_bytes,
                    parsing_options=parsing_options,
                    processors=processors,
                    cache=cache,
                )
            case "content_length":
                self._parser = ContentLengthWARCParser(
                    file_handle=file_handle,
                    enable_lazy_loading_of_bytes=enable_lazy_loading_of_bytes,
                    parsing_options=parsing_options,
                    processors=processors,
                    cache=cache,
                )
            case _:
                supported_parsing_styles = [
                    "delimiter",
                    "content_length",
                ]
                raise ValueError(
                    f"Supported parsing styles: {', '.join(supported_parsing_styles)}"
                )

    @property
    def warnings(self) -> List[str]:
        return self._parser.warnings

    @property
    def error(self) -> Optional[str]:
        return self._parser.error

    @property
    def current_record(self) -> Optional["Record"]:
        return self._parser.current_record

    @property
    def records(self) -> List["Record"]:
        return self._parser.records

    @property
    def unparsable_lines(self) -> List["UnparsableLine"]:
        return self._parser.unparsable_lines

    def parse(self, cache_records: bool = True) -> None:
        """
        Parse the entire WARC file and optionally cache all records in memory.

        Args:
            cache_records: If True, store all parsed records in memory for later access
                via the `records` property. If False, records are only accessible during
                the parsing process; all processing must be done by record handlers.
        """
        return self._parser.parse(cache_records)

    def iterator(self) -> Iterator["Record"]:
        """
        Return an iterator that yields Record objects one at a time.

        Returns:
            Iterator[Record]: An iterator of WARC record objects
        """
        return self._parser.iterator()

    def get_record_offsets(
        self, split: bool = False
    ) -> Union[List[Tuple[int, int]], List[Tuple[int, int, int, int]]]:
        """
        Get the byte offsets of all records in the file.

        Args:
            split: If True, return separate offsets for headers and content blocks.
                If False, return offsets for complete records.

        Returns:
            List of tuples containing byte offsets. If split=False, each tuple
            contains (start, end). If split=True, each tuple contains
            (header_start, header_end, content_start, content_end).
        """
        return self._parser.get_record_offsets(split)

    def get_approximate_request_response_pairs(
        self, count_only: bool = False
    ) -> Dict[str, Any]:
        """
        Identify and match HTTP request/response pairs in the WARC file.
        Only approximate: if multiple requests were made to the same Target-URI,
        matching may be incorrect.

        Args:
            count_only: If True, return only counts. If False, return detailed
                information about the pairs.

        Returns:
            Dict containing information about request/response pairs found.
            Structure depends on count_only parameter.
        """
        return self._parser.get_approximate_request_response_pairs(count_only)


class WARCGZParser:
    """
    A parser for gzipped WARC files.

    This parser handles WARC files that have been compressed with gzip, including those
    within WACZ files.

    Args:
        file_handle: A file-like object opened in binary mode
        enable_lazy_loading_of_bytes: Whether to enable on-demand byte access (default: True).
            When True, attaches file handles to parsed objects so bytes can be read
            on-demand when accessed. When False, bytes are only accessible if explicitly
            cached via the cache configuration. Independent of cache settings.
        parsing_options: Configuration for parsing behavior
        processors: Configuration for filters, handlers, and callbacks
        cache: Configuration for what data to cache in memory during parsing.
            Controls immediate caching of bytes, parsed headers, etc. Independent of
            enable_lazy_loading_of_bytes. When both are enabled, you get both immediate
            caching AND on-demand access capabilities.
    """

    def __init__(
        self,
        file_handle: BufferedReader,
        enable_lazy_loading_of_bytes: bool = True,
        parsing_options: Optional[WARCGZParsingConfig] = None,
        processors: Optional[WARCGZProcessorConfig] = None,
        cache: Optional[WARCGZCachingConfig] = None,
    ):
        # Set up default config
        if parsing_options is None:
            parsing_options = WARCGZParsingConfig()
        if cache is None:
            cache = WARCGZCachingConfig()
        if processors is None:
            processors = WARCGZProcessorConfig()

        # Validate config
        if (
            enable_lazy_loading_of_bytes
            and parsing_options.decompression_style != "file"
        ):
            raise ValueError(
                "The lazy loading of bytes is only supported when decompression style is 'file'."
            )

        # Initialize the appropriate parser
        self._parser: Union[GzippedWARCMemberParser, GzippedWARCDecompressingParser]
        match parsing_options.style:
            case "split_gzip_members":
                if parsing_options.decompression_style == "member":
                    self._parser = GzippedWARCMemberParser(
                        file_handle=file_handle,
                        enable_lazy_loading_of_bytes=False,
                        parsing_options=parsing_options,
                        processors=processors,
                        cache=cache,
                    )
                elif parsing_options.decompression_style == "file":
                    self._parser = GzippedWARCDecompressingParser(
                        file_handle=file_handle,
                        enable_lazy_loading_of_bytes=enable_lazy_loading_of_bytes,
                        parsing_options=parsing_options,
                        processors=processors,
                        cache=cache,
                    )
                else:
                    supported_decompression_styles = [
                        "member",
                        "file",
                    ]
                    raise ValueError(
                        f"Supported decompression styles: {', '.join(supported_decompression_styles)}"
                    )
            case _:
                supported_parsing_styles = [
                    "split_gzip_members",
                ]
                raise ValueError(
                    f"Supported parsing styles: {', '.join(supported_parsing_styles)}"
                )

    @property
    def warnings(self) -> List[str]:
        return self._parser.warnings

    @property
    def error(self) -> Optional[str]:
        return self._parser.error

    @property
    def current_member(self) -> Optional["GzippedMember"]:
        return self._parser.current_member

    @property
    def members(self) -> List["GzippedMember"]:
        return self._parser.members

    @property
    def records(self) -> List["Record"]:
        return self._parser.records

    def parse(self, cache_members: bool = True) -> None:
        """
        Parse the entire gzipped WARC file and optionally cache all members in memory.

        Args:
            cache_members: If True, store all parsed gzip members in memory for later
                access via the `members` property. If False, members are only accessible
                during the parsing process; all processing must be done by member or
                record handlers.
        """
        return self._parser.parse(cache_members)

    def iterator(
        self, yield_type: str = "members"
    ) -> Union[Iterator["GzippedMember"], Iterator["Record"]]:
        """
        Return an iterator that yields either gzip members or WARC records.

        Args:
            yield_type: Either "members" to yield GzippedMember objects, or "records"
                to yield Record objects extracted from successfully parsed members.

        Returns:
            Iterator[GzippedMember] or Iterator[Record]: An iterator of the requested type
        """
        return self._parser.iterator(yield_type)

    def get_member_offsets(
        self, compressed: bool = True
    ) -> List[Tuple[Optional[int], Optional[int]]]:
        """
        Get the byte offsets of all gzip members in the file.

        Args:
            compressed: If True, return offsets in the compressed file. If False,
                return offsets as they would appear in the decompressed file.

        Returns:
            List of tuples containing (start, end) byte offsets for each gzip member.
        """
        return self._parser.get_member_offsets(compressed)

    def get_record_offsets(
        self, split: bool = False
    ) -> Union[List[Tuple[int, int]], List[Tuple[int, int, int, int]]]:
        """
        Get the byte offsets of all WARC records extracted from gzip members.

        Args:
            split: If True, return separate offsets for headers and content blocks.
                If False, return offsets for complete records.

        Returns:
            List of tuples containing byte offsets. If split=False, each tuple
            contains (start, end). If split=True, each tuple contains
            (header_start, header_end, content_start, content_end).
        """
        return self._parser.get_record_offsets(split)

    def get_approximate_request_response_pairs(
        self, count_only: bool = False
    ) -> Dict[str, Any]:
        """
        Identify and match HTTP request/response pairs in the extracted WARC records.
        Only approximate: if multiple requests were made to the same Target-URI,
        matching may be incorrect.

        Args:
            count_only: If True, return only counts. If False, return detailed
                information about the pairs.

        Returns:
            Dict containing information about request/response pairs found.
            Structure depends on count_only parameter.
        """
        return self._parser.get_approximate_request_response_pairs(count_only)
