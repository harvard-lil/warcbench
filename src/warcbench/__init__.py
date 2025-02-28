import gzip
import zipfile

from warcbench.exceptions import AttributeNotInitializedError
from warcbench.models import Record, UnparsableLine
from warcbench.logging import logging
from warcbench.patterns import CRLF, WARC_VERSION
from warcbench.utils import (
    skip_leading_whitespace,
    find_next_delimiter,
    find_content_length_end,
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


class WARCParser:
    def __init__(
        self,
        file_handle,
        parsing_style="delimiter",
        parsing_chunk_size=1024,
        check_content_lengths=False,
        cache_unparsable_lines=False,
        cache_record_bytes=False,
        cache_header_bytes=False,
        cache_content_block_bytes=False,
        cache_unparsable_line_bytes=False,
        enable_lazy_loading_of_bytes=True,
        filters=None,
        unparsable_line_handlers=None,
    ):
        # Validate Options
        supported_parsing_styles = ["delimiter", "content_length"]
        if parsing_style not in supported_parsing_styles:
            raise ValueError(
                f"Supported parsing styles: {', '.join(supported_parsing_styles)}"
            )

        if check_content_lengths:
            if parsing_style == "content_length":
                raise ValueError(
                    "Checking content lengths is only meaningful when parsing in delimter mode."
                )

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

        self.state = STATES["FIND_WARC_HEADER"]
        self.transitions = {
            STATES["FIND_WARC_HEADER"]: self.find_warc_header,
            STATES["FIND_NEXT_RECORD"]: self.find_next_record,
            STATES["EXTRACT_NEXT_RECORD"]: self.extract_next_record,
            STATES["CHECK_RECORD_AGAINST_FILTERS"]: self.check_record_against_filters,
            STATES["END"]: None,
        }

        self.file_handle = file_handle
        self.check_content_lengths = check_content_lengths
        self.cache_unparsable_lines = cache_unparsable_lines
        self.cache_record_bytes = cache_record_bytes
        self.cache_header_bytes = cache_header_bytes
        self.cache_content_block_bytes = cache_content_block_bytes
        self.cache_unparsable_line_bytes = cache_unparsable_line_bytes
        self.enable_lazy_loading_of_bytes = enable_lazy_loading_of_bytes
        self.filters = filters
        self.unparsable_line_handlers = unparsable_line_handlers
        self.parsing_style = parsing_style
        self.parsing_chunk_size = parsing_chunk_size

        self.warnings = []
        self.error = None
        self.current_record = None

        self._records = None
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
        initial_position = self.file_handle.tell()
        first_line = self.file_handle.readline()
        self.file_handle.seek(initial_position)

        if first_line == WARC_VERSION:
            return STATES["EXTRACT_NEXT_RECORD"]
        else:
            self.error = "No WARC header found."
            return STATES["END"]

    def extract_next_record(self):
        start = self.file_handle.tell()
        stop = self.find_record_end()
        if stop:
            # Don't include the delimiter in the record's data or offsets
            end = stop - len(CRLF * 2)
            data = self.file_handle.read(end - start)
            # Advance the cursor past the delimiter
            if self.file_handle.peek(len(CRLF * 2)).startswith(CRLF * 2):
                self.file_handle.read(len(CRLF * 2))
            else:
                self.warnings.append(f"The record between {start}-{end} was improperly terminated.")
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

    def find_record_end(self):
        match self.parsing_style:
            case "delimiter":
                return find_next_delimiter(self.file_handle, self.parsing_chunk_size)
            case "content_length":
                return find_content_length_end(
                    self.file_handle, self.parsing_chunk_size
                )
            case _:
                raise NotImplementedError()


def main() -> None:
    #
    # Example Usage
    #

    with (
        open("assets/example.com.wacz", "rb") as wacz_file,
        zipfile.Path(wacz_file, "archive/data.warc.gz").open("rb") as warc_gz_file,
        gzip.open(warc_gz_file, "rb") as warc_file,
    ):
        parser = WARCParser(
            warc_file,
            # parsing_style="content_length",
            check_content_lengths=True,
            cache_unparsable_lines=True,
            # cache_record_bytes=True,
            # cache_header_bytes=True,
            # cache_content_block_bytes=True,
            # cache_unparsable_line_bytes=True,
            # enable_lazy_loading_of_bytes=False,
            filters=[
                # lambda record: False,
                # record_content_length_filter(1007),
                # record_content_length_filter(38978, 'gt'),
                # record_content_type_filter('http'),
                # warc_named_field_filter('type', 'warcinfo'),
                # warc_named_field_filter('type', 'request'),
                # warc_named_field_filter('target-uri', 'favicon'),
                # warc_named_field_filter(
                #     'target-uri',
                #     'http://example.com/',
                #     exact_match=True
                # ),
                # http_verb_filter('get'),
                # http_status_filter(200),
                # http_header_filter('content-encoding', 'gzip'),
                # http_response_content_type_filter('pdf'),
                # warc_header_regex_filter('Scoop-Exchange-Description: Provenance Summary'),
            ],
            # unparsable_line_handlers=[
            #     lambda line: print(len(line.bytes))
            # ]
        )
        parser.parse(
            # find_first_record_only=True,
        )
        print(len(parser.records))
        # for record in parser.records:
        # print(record.get_http_header_block())
        # print(record.get_http_body_block())
        # print("\n\n")
        breakpoint()
