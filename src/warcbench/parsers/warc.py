"""
`parsers.warc` module: Classes that slice a WARC into pieces, using different strategies

This module implements a state machine-based parser for WARC files. The parsing process
follows a sequence of states defined in the STATES dictionary below. See the BaseParser
class for the state machine implementation and the iterator() method for how states
transition.

The inheritance pattern allows for different parsing strategies:
- BaseParser: Abstract base class defining the state machine and common functionality
- Concrete subclasses: Implement different strategies for finding record boundaries
"""

from abc import ABC, abstractmethod
from io import BufferedReader
import logging
import os
from typing import List, Optional

from warcbench.exceptions import AttributeNotInitializedError, SplitRecordsRequiredError
from warcbench.models import Record, Header, ContentBlock, UnparsableLine
from warcbench.patterns import CRLF, WARC_VERSIONS
from warcbench.utils import (
    skip_leading_whitespace,
    advance_to_next_line,
    find_next_delimiter,
    find_next_header_end,
    find_content_length_in_bytes,
    find_matching_request_response_pairs,
)
from warcbench.config import WARCCachingConfig, WARCProcessorConfig, WARCParsingConfig

logger = logging.getLogger(__name__)


STATES = {
    "FIND_WARC_HEADER": "find_warc_header",
    "EXTRACT_NEXT_RECORD": "extract_next_record",
    "CHECK_RECORD_AGAINST_FILTERS": "check_record_against_filters",
    "RUN_RECORD_HANDLERS": "run_record_handlers",
    "YIELD_CURRENT_RECORD": "yield_record",
    "FIND_NEXT_RECORD": "find_next_record",
    "RUN_PARSER_CALLBACKS": "run_parser_callbacks",
    "END": "end",
}


class BaseParser(ABC):
    """
    Abstract base class for WARC parsers implementing a state machine.

    This class provides the core parsing infrastructure including:
    - State machine implementation for driving the parsing process
    - Common functionality for filters, handlers, and callbacks
    - Record caching and iteration capabilities

    Subclasses must implement the `extract_next_record()` method to define
    how individual WARC records are parsed from the file.
    """

    def __init__(
        self,
        file_handle: BufferedReader,
        enable_lazy_loading_of_bytes: bool,
        parsing_options: WARCParsingConfig,
        processors: WARCProcessorConfig,
        cache: WARCCachingConfig,
    ):
        self.state = STATES["FIND_WARC_HEADER"]
        self.transitions = {
            STATES["FIND_WARC_HEADER"]: self.find_warc_header,
            STATES["FIND_NEXT_RECORD"]: self.find_next_record,
            STATES["EXTRACT_NEXT_RECORD"]: self.extract_next_record,
            STATES["CHECK_RECORD_AGAINST_FILTERS"]: self.check_record_against_filters,
            STATES["RUN_RECORD_HANDLERS"]: self.run_record_handlers,
            STATES["RUN_PARSER_CALLBACKS"]: self.run_parser_callbacks,
            STATES["END"]: None,
        }

        self.file_handle = file_handle
        self.enable_lazy_loading_of_bytes = enable_lazy_loading_of_bytes
        self.parsing_options = parsing_options
        self.cache = cache
        self.processors = processors

        self.warnings: List[str] = []
        self.error: Optional[str] = None
        self.current_record: Optional[Record] = None

        self._records: Optional[List[Record]] = None

        self._unparsable_lines: Optional[List[UnparsableLine]]
        if cache.unparsable_lines:
            self._unparsable_lines = []
        else:
            self._unparsable_lines = None

    @property
    def records(self):
        if self._records is None:
            raise AttributeNotInitializedError(
                "Call parser.parse(cache_members=True) to load records into RAM and populate parser.records, "
                "or use parser.iterator() to iterate through records without preloading."
            )
        return self._records

    @property
    def unparsable_lines(self):
        if self._unparsable_lines is None:
            raise AttributeNotInitializedError(
                "Pass cache_unparsable_lines=True to WARCParser() to store UnparsableLines "
                "in parser.unparsable_lines."
            )
        return self._unparsable_lines

    def parse(self, cache_records):
        if cache_records:
            self._records = []

        iterator = self.iterator()
        for record in iterator:
            if cache_records:
                self._records.append(record)  # type: ignore[union-attr]

    def iterator(self):
        yielded = 0
        self.file_handle.seek(0)

        while self.state != STATES["END"]:
            if self.state == STATES["YIELD_CURRENT_RECORD"]:
                yielded = yielded + 1
                yield self.current_record
                self.current_record = None

                if (
                    self.parsing_options.stop_after_nth
                    and yielded >= self.parsing_options.stop_after_nth
                ):
                    logger.debug(
                        f"Stopping early after yielding {self.parsing_options.stop_after_nth} records."
                    )
                    self.state = STATES["RUN_PARSER_CALLBACKS"]
                    continue

                self.state = STATES["FIND_NEXT_RECORD"]
            else:
                transition_func = self.transitions[self.state]
                if not transition_func:
                    raise RuntimeError(
                        f"Parser logic error: {self.state} has no transition function."
                    )
                self.state = transition_func()

    def get_record_offsets(self, split):
        records = self._records if self._records else self.iterator()

        if split:
            if not self.parsing_options.split_records:
                raise ValueError(
                    "Split record offsets are only available when the parser is initialized with split_records=True."
                )
            return [
                (
                    record.header.start,  # type: ignore[union-attr]
                    record.header.end,  # type: ignore[union-attr]
                    record.content_block.start,  # type: ignore[union-attr]
                    record.content_block.end,  # type: ignore[union-attr]
                )
                for record in records
            ]

        return [(record.start, record.end) for record in records]

    def get_approximate_request_response_pairs(self, count_only):
        """
        Recommended: use with cache_parsed_headers=True.
        """
        records = self._records if self._records else self.iterator()
        return find_matching_request_response_pairs(records, count_only)

    #
    # Internal Methods
    #

    def find_warc_header(self):
        skip_leading_whitespace(self.file_handle)
        for warc_version in WARC_VERSIONS:
            header_found = self.file_handle.peek(len(warc_version)).startswith(
                warc_version
            )
            if header_found:
                return STATES["EXTRACT_NEXT_RECORD"]
        self.error = "No WARC header found."
        return STATES["RUN_PARSER_CALLBACKS"]

    def find_next_record(self):
        while True:
            initial_position = self.file_handle.tell()
            for warc_version in WARC_VERSIONS:
                if self.file_handle.peek(len(warc_version)).startswith(warc_version):
                    return STATES["EXTRACT_NEXT_RECORD"]

            next_line = advance_to_next_line(self.file_handle)
            current_position = self.file_handle.tell()
            if next_line:
                unparsable_line = UnparsableLine(
                    start=initial_position,
                    end=current_position,
                )
                if self.cache.unparsable_line_bytes:
                    self.file_handle.seek(initial_position)
                    unparsable_line._bytes = self.file_handle.read(
                        current_position - initial_position
                    )
                if self.enable_lazy_loading_of_bytes:
                    unparsable_line._file_handle = self.file_handle
                if self.processors.unparsable_line_handlers:
                    for handler in self.processors.unparsable_line_handlers:
                        handler(unparsable_line)
                if self.cache.unparsable_lines:
                    self.unparsable_lines.append(unparsable_line)
            else:
                return STATES["RUN_PARSER_CALLBACKS"]

    def check_record_against_filters(self):
        if self.current_record is None:
            raise RuntimeError(
                "Parser logic error: check_record_against_filters called with no current record."
            )

        retained = True
        if self.processors.record_filters:
            for f in self.processors.record_filters:
                if not f(self.current_record):
                    retained = False
                    logger.debug(
                        f"Skipping record at {self.current_record.start}-{self.current_record.end} due to filter."
                    )
                    break

        if retained:
            return STATES["RUN_RECORD_HANDLERS"]
        return STATES["FIND_NEXT_RECORD"]

    def run_record_handlers(self):
        if self.current_record is None:
            raise RuntimeError(
                "Parser logic error: run_record_handlers called with no current record."
            )

        if self.processors.record_handlers:
            for f in self.processors.record_handlers:
                f(self.current_record)

        return STATES["YIELD_CURRENT_RECORD"]

    def run_parser_callbacks(self):
        if self.processors.parser_callbacks:
            for f in self.processors.parser_callbacks:
                f(self)

        return STATES["END"]

    @abstractmethod
    def extract_next_record(self):
        pass


class DelimiterWARCParser(BaseParser):
    """
    WARC parser that looks for the WARC record delimiter pattern (\r\n\r\n) to
    determine where one record ends and the next begins.
    """

    def __init__(
        self,
        file_handle: BufferedReader,
        enable_lazy_loading_of_bytes: bool,
        parsing_options: WARCParsingConfig,
        processors: WARCProcessorConfig,
        cache: WARCCachingConfig,
    ):
        #
        # Validate Options
        #

        if parsing_options.check_content_lengths:
            if not parsing_options.split_records:
                raise ValueError("To check_content_lengths, you must split records.")

            if not enable_lazy_loading_of_bytes and not all(
                [cache.header_bytes, cache.content_block_bytes]
            ):
                raise ValueError(
                    "To check_content_lengths, you must either enable_lazy_loading_of_bytes or "
                    "both cache_header_bytes and cache_content_block_bytes."
                )

        if cache.header_bytes or cache.parsed_headers or cache.content_block_bytes:
            if not parsing_options.split_records:
                raise ValueError(
                    "To cache or parse header or content block bytes, you must split records."
                )

        #
        # Set Up
        #

        super().__init__(
            file_handle=file_handle,
            enable_lazy_loading_of_bytes=enable_lazy_loading_of_bytes,
            parsing_options=parsing_options,
            processors=processors,
            cache=cache,
        )

    def extract_next_record(self):
        start = self.file_handle.tell()
        stop = find_next_delimiter(
            self.file_handle, self.parsing_options.parsing_chunk_size
        )
        if stop:
            # Don't include the delimiter in the record's data or offsets
            end = stop - len(CRLF * 2)
        else:
            self.warnings.append("Last record may have been truncated.")
            end = self.file_handle.tell()

        record = Record(start=start, end=end)
        if self.cache.record_bytes:
            record._bytes = self.file_handle.read(record.length)
        else:
            self.file_handle.seek(end)
        if self.enable_lazy_loading_of_bytes:
            record._file_handle = self.file_handle

        if self.parsing_options.split_records:
            header_start = record.start
            self.file_handle.seek(header_start)
            header_with_linebreak_end = find_next_header_end(
                self.file_handle, self.parsing_options.parsing_chunk_size
            )

            if header_with_linebreak_end:
                # Don't include the line break in the header's data or offsets
                header_end = header_with_linebreak_end - len(CRLF)

                content_block_start = header_end + len(CRLF)
                content_block_end = record.end

                record.header = Header(start=header_start, end=header_end)
                if self.cache.header_bytes or self.cache.parsed_headers:
                    header_bytes = self.file_handle.read(record.header.length)

                    if self.cache.header_bytes:
                        record.header._bytes = header_bytes

                    if self.cache.parsed_headers:
                        record.header._parsed_fields = (
                            record.header.parse_bytes_into_fields(header_bytes)
                        )

                if self.enable_lazy_loading_of_bytes:
                    record.header._file_handle = self.file_handle

                record.content_block = ContentBlock(
                    start=content_block_start,
                    end=content_block_end,
                )
                if self.cache.content_block_bytes:
                    self.file_handle.seek(content_block_start)
                    record.content_block._bytes = self.file_handle.read(
                        record.content_block.length
                    )
                else:
                    self.file_handle.seek(content_block_end)
                if self.enable_lazy_loading_of_bytes:
                    record.content_block._file_handle = self.file_handle

                if self.parsing_options.check_content_lengths:
                    record.check_content_length()

            else:
                self.warnings.append(
                    f"Could not split the record between {record.start} and {record.end} "
                    "into header and content block components."
                )

        # Advance the cursor
        self.file_handle.read(len(CRLF * 2))

        self.current_record = record
        return STATES["CHECK_RECORD_AGAINST_FILTERS"]


class ContentLengthWARCParser(BaseParser):
    """
    WARC parser that reads each WARC header, extracts the Content-Length value,
    then skips exactly that many bytes to find the next record.
    """

    def extract_next_record(self):
        #
        # Find what looks like the next WARC header record
        #

        header_start = self.file_handle.tell()
        header_with_linebreak_end = find_next_header_end(
            self.file_handle, self.parsing_options.parsing_chunk_size
        )
        if header_with_linebreak_end:
            # Don't include the line break in the header's data or offsets
            header_end = header_with_linebreak_end - len(CRLF)
            header_bytes = self.file_handle.read(header_end - header_start)
            self.file_handle.read(len(CRLF))
        else:
            header_bytes = self.file_handle.read()
            header_end = self.file_handle.tell()

        #
        # Try to extract the value of the mandatory Content-Length field
        #

        content_length = find_content_length_in_bytes(header_bytes)

        #
        # If we can't, then this block isn't parsable as a WARC record using this strategy
        #

        if not content_length:
            start_index = header_start
            for line in header_bytes.split(b"\r\n"):
                end_index = start_index + len(line) + 2
                unparsable_line = UnparsableLine(
                    start=start_index,
                    end=end_index,
                )
                start_index = end_index
                if self.cache.unparsable_line_bytes:
                    unparsable_line._bytes = line + b"\r\n"
                if self.enable_lazy_loading_of_bytes:
                    unparsable_line._file_handle = self.file_handle
                if self.processors.unparsable_line_handlers:
                    for handler in self.processors.unparsable_line_handlers:
                        handler(unparsable_line)
                if self.cache.unparsable_lines:
                    self.unparsable_lines.append(unparsable_line)
            return STATES["FIND_NEXT_RECORD"]

        content_start = self.file_handle.tell()
        if self.cache.content_block_bytes or self.cache.record_bytes:
            content_bytes = self.file_handle.read(content_length)
        else:
            self.file_handle.seek(content_length, os.SEEK_CUR)
            content_bytes = None
        content_end = self.file_handle.tell()

        #
        # Build the Record object
        #
        record = Record(start=header_start, end=content_end)
        if self.cache.record_bytes:
            data = bytearray()
            data.extend(header_bytes)
            data.extend(b"\n")
            data.extend(content_bytes)  # type: ignore[arg-type]
            record._bytes = bytes(data)
        if self.enable_lazy_loading_of_bytes:
            record._file_handle = self.file_handle

        if self.parsing_options.split_records:
            header = Header(start=header_start, end=header_end)
            if self.cache.header_bytes:
                header._bytes = header_bytes
            if self.enable_lazy_loading_of_bytes:
                header._file_handle = self.file_handle
            if self.cache.parsed_headers:
                header._parsed_fields = header.parse_bytes_into_fields(header_bytes)

            content_block = ContentBlock(start=content_start, end=content_end)
            if self.cache.content_block_bytes:
                content_block._bytes = content_bytes
            if self.enable_lazy_loading_of_bytes:
                content_block._file_handle = self.file_handle

            record.header = header
            record.content_block = content_block

        #
        # Advance the cursor past the expected WARC record delimiter
        #
        if self.file_handle.peek(len(CRLF * 2)).startswith(CRLF * 2):
            self.file_handle.read(len(CRLF * 2))
        else:
            self.warnings.append(
                f"The record between {header_start}-{content_end} was improperly terminated."
            )

        self.current_record = record
        return STATES["CHECK_RECORD_AGAINST_FILTERS"]
