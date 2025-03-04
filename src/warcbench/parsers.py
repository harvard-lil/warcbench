"""
`parsers` module: Classes that slice a WARC into pieces, using different strategies
"""

from abc import ABC, abstractmethod
import logging
import os

from warcbench.exceptions import AttributeNotInitializedError
from warcbench.models import Record, Header, ContentBlock, UnparsableLine
from warcbench.patterns import CRLF, WARC_VERSION
from warcbench.utils import (
    skip_leading_whitespace,
    find_next_delimiter,
    find_next_header_end,
    find_content_length_in_bytes,
)

logger = logging.getLogger(__name__)


STATES = {
    "FIND_WARC_HEADER": "find_warc_header",
    "EXTRACT_NEXT_RECORD": "extract_next_record",
    "CHECK_RECORD_AGAINST_FILTERS": "check_record_against_filters",
    "YIELD_CURRENT_RECORD": "yield_record",
    "FIND_NEXT_RECORD": "find_next_record",
    "END": "end",
}


class BaseParser(ABC):
    warnings = []
    error = None
    current_record = None
    _records = None

    def __init__(
        self,
        file_handle,
        parsing_chunk_size,
        cache_unparsable_lines,
        cache_record_bytes,
        cache_header_bytes,
        cache_content_block_bytes,
        cache_unparsable_line_bytes,
        enable_lazy_loading_of_bytes,
        filters,
        unparsable_line_handlers,
    ):
        self.state = STATES["FIND_WARC_HEADER"]
        self.transitions = {
            STATES["FIND_WARC_HEADER"]: self.find_warc_header,
            STATES["FIND_NEXT_RECORD"]: self.find_next_record,
            STATES["EXTRACT_NEXT_RECORD"]: self.extract_next_record,
            STATES["CHECK_RECORD_AGAINST_FILTERS"]: self.check_record_against_filters,
            STATES["END"]: None,
        }

        self.file_handle = file_handle
        self.cache_unparsable_lines = cache_unparsable_lines
        self.cache_record_bytes = cache_record_bytes
        self.cache_header_bytes = cache_header_bytes
        self.cache_content_block_bytes = cache_content_block_bytes
        self.cache_unparsable_line_bytes = cache_unparsable_line_bytes
        self.enable_lazy_loading_of_bytes = enable_lazy_loading_of_bytes
        self.filters = filters
        self.unparsable_line_handlers = unparsable_line_handlers
        self.parsing_chunk_size = parsing_chunk_size

        if cache_unparsable_lines:
            self._unparsable_lines = []
        else:
            self._unparsable_lines = None

    @property
    def records(self):
        if self._records is None:
            raise AttributeNotInitializedError(
                "Call parser.parse() to load records into RAM and populate parser.records, "
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

    def parse(self, find_first_record_only=False):
        self._records = []
        iterator = self.iterator(find_first_record_only=find_first_record_only)
        for record in iterator:
            self._records.append(record)

    def iterator(self, find_first_record_only=False):
        self.file_handle.seek(0)

        while self.state != STATES["END"]:
            if self.state == STATES["YIELD_CURRENT_RECORD"]:
                yield self.current_record
                self.current_record = None

                if find_first_record_only:
                    self.state = STATES["END"]
                    continue

                self.state = STATES["FIND_NEXT_RECORD"]
            else:
                transition_func = self.transitions[self.state]
                self.state = transition_func()

    def find_warc_header(self):
        skip_leading_whitespace(self.file_handle)
        header_found = self.file_handle.peek(len(WARC_VERSION)).startswith(WARC_VERSION)
        if header_found:
            return STATES["EXTRACT_NEXT_RECORD"]
        else:
            self.error = "No WARC header found."
            return STATES["END"]

    def find_next_record(self):
        while True:
            initial_position = self.file_handle.tell()
            if self.file_handle.peek(len(WARC_VERSION)).startswith(WARC_VERSION):
                return STATES["EXTRACT_NEXT_RECORD"]

            next_line = self.file_handle.readline()
            current_position = self.file_handle.tell()
            if next_line:
                unparsable_line = UnparsableLine(
                    start=initial_position,
                    end=current_position,
                )
                if self.cache_unparsable_line_bytes:
                    unparsable_line._bytes = next_line
                if self.enable_lazy_loading_of_bytes:
                    unparsable_line._file_handle = self.file_handle
                if self.unparsable_line_handlers:
                    for handler in self.unparsable_line_handlers:
                        handler(unparsable_line)
                if self.cache_unparsable_lines:
                    self.unparsable_lines.append(unparsable_line)
            else:
                return STATES["END"]

    def check_record_against_filters(self):
        retained = True
        if self.filters:
            for f in self.filters:
                if not f(self.current_record):
                    retained = False
                    logger.debug(
                        f"Skipping record at {self.current_record.start}-{self.current_record.end} due to filter."
                    )
                    break

        if retained:
            return STATES["YIELD_CURRENT_RECORD"]
        return STATES["FIND_NEXT_RECORD"]

    @abstractmethod
    def extract_next_record(self):
        pass


class DelimiterWARCParser(BaseParser):
    def __init__(
        self,
        file_handle,
        parsing_chunk_size,
        check_content_lengths,
        cache_unparsable_lines,
        cache_record_bytes,
        cache_header_bytes,
        cache_content_block_bytes,
        cache_unparsable_line_bytes,
        enable_lazy_loading_of_bytes,
        filters,
        unparsable_line_handlers,
    ):
        #
        # Validate Options
        #

        if check_content_lengths:
            if not enable_lazy_loading_of_bytes and not all(
                [cache_header_bytes, cache_content_block_bytes]
            ):
                raise ValueError(
                    "To check_content_lengths, you must either enable_lazy_loading_of_bytes or "
                    "both cache_header_bytes and cache_content_block_bytes."
                )

        #
        # Set Up
        #

        self.check_content_lengths = check_content_lengths

        super().__init__(
            file_handle,
            parsing_chunk_size,
            cache_unparsable_lines,
            cache_record_bytes,
            cache_header_bytes,
            cache_content_block_bytes,
            cache_unparsable_line_bytes,
            enable_lazy_loading_of_bytes,
            filters,
            unparsable_line_handlers,
        )

    def extract_next_record(self):
        start = self.file_handle.tell()
        stop = find_next_delimiter(self.file_handle, self.parsing_chunk_size)
        if stop:
            # Don't include the delimiter in the record's data or offsets
            end = stop - len(CRLF * 2)
            data = self.file_handle.read(end - start)
            self.file_handle.read(len(CRLF * 2))
        else:
            self.warnings.append("Last record may have been truncated.")
            data = self.file_handle.read()
            end = self.file_handle.tell()

        record = Record(start=start, end=end)
        if self.cache_record_bytes:
            record._bytes = data
        if self.enable_lazy_loading_of_bytes:
            record._file_handle = self.file_handle
        record.split(
            data,
            cache_header_bytes=self.cache_header_bytes,
            cache_content_block_bytes=self.cache_content_block_bytes,
            enable_lazy_loading_of_bytes=self.enable_lazy_loading_of_bytes,
        )

        if self.check_content_lengths:
            record.check_content_length()
        self.current_record = record
        return STATES["CHECK_RECORD_AGAINST_FILTERS"]


class ContentLengthWARCParser(BaseParser):
    def extract_next_record(self):
        #
        # Find what looks like the next WARC header record
        #

        header_start = self.file_handle.tell()
        header_with_linkbreak_end = find_next_header_end(
            self.file_handle, self.parsing_chunk_size
        )
        if header_with_linkbreak_end:
            # Don't include the line break in the header's data or offsets
            header_end = header_with_linkbreak_end - len(CRLF)
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
            for line in header_bytes.split(b"\n"):
                end_index = start_index + len(line) + 1
                unparsable_line = UnparsableLine(
                    start=start_index,
                    end=end_index,
                )
                if self.cache_unparsable_line_bytes:
                    unparsable_line._bytes = line + b"\n"
                if self.enable_lazy_loading_of_bytes:
                    unparsable_line._file_handle = self.file_handle
                if self.unparsable_line_handlers:
                    for handler in self.unparsable_line_handlers:
                        handler(unparsable_line)
                if self.cache_unparsable_lines:
                    self.unparsable_lines.append(unparsable_line)
            return STATES["FIND_NEXT_RECORD"]

        content_start = self.file_handle.tell()
        if self.cache_content_block_bytes or self.cache_record_bytes:
            content_bytes = self.file_handle.read(content_length)
        else:
            self.file_handle.seek(content_length, os.SEEK_CUR)
            content_bytes = None
        content_end = self.file_handle.tell()

        #
        # Build the Record object
        #

        header = Header(start=header_start, end=header_end)
        if self.cache_header_bytes:
            header._bytes = header_bytes
        if self.enable_lazy_loading_of_bytes:
            header._file_handle = self.file_handle

        content_block = ContentBlock(start=content_start, end=content_end)
        if self.cache_content_block_bytes:
            content_block._bytes = content_bytes
        if self.enable_lazy_loading_of_bytes:
            content_block._file_handle = self.file_handle

        record = Record(start=header_start, end=content_end)
        record.header = header
        record.content_block = content_block
        if self.cache_record_bytes:
            data = bytearray()
            data.extend(header_bytes)
            data.extend(b"\n")
            data.extend(content_bytes)
            record._bytes = bytes(data)
        if self.enable_lazy_loading_of_bytes:
            record._file_handle = self.file_handle

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
