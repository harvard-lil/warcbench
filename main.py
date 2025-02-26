from dataclasses import dataclass, field
import functools
import gzip
import io
import logging
import operator
import os
import re
from typing import Optional
import zipfile

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')


#
# Constants and patterns
#

CRLF = b"\r\n"
WARC_VERSION = b"WARC/1.1\r\n"
CONTENT_LENGTH_PATTERN = rb"Content-Length:\s*(\d+)"
CONTENT_TYPE_PATTERN = rb"Content-Type:\s*(.*)((\r\n)|$)"


def get_warc_named_field_pattern(field_name):
    return b"WARC-" + bytes(field_name, "utf-8") + rb":\s*(.*)((\r\n)|$)"


def get_http_verb_pattern(verb):
    return bytes(f"({verb})", "utf-8") + rb"\s+.*HTTP/.*((\r\n)|$)"


def get_http_status_pattern(status_code):
    return rb"HTTP/1.1\s*" + bytes(f"({status_code})", "utf-8")


def get_http_header_pattern(header_name):
    return bytes(header_name, "utf-8") + rb":\s*(.+)((\r\n)|$)"


#
# Models
#

@dataclass
class ByteRange():
    start: int
    end: int
    _bytes: Optional[bytes] = field(repr=False, default=None)
    _file_handle: Optional[io.BufferedReader] = field(repr=False, default=None)

    def __post_init__(self):
        self.length = self.end - self.start

    @property
    def bytes(self):
        if self._bytes is None:
            if not self._file_handle:
                raise ValueError(
                    "To access record bytes, you must either enable_lazy_loading_of_bytes or "
                    "cache_record_bytes/cache_header_bytes/cache_content_block_bytes."
                )
            logging.debug(f"Reading from {self.start} to {self.end}.")
            original_postion = self._file_handle.tell()
            self._file_handle.seek(self.start)
            data = self._file_handle.read(self.length)
            self._file_handle.seek(original_postion)
            return data
        return self._bytes


@dataclass
class UnparsableLine(ByteRange):
    pass


@dataclass
class Record(ByteRange):

    content_length_check_result: Optional[int] = None

    def check_content_length(self):
        match = find_pattern_in_bytes(CONTENT_LENGTH_PATTERN, self.header.bytes)

        if match:
            expected = int(match.group(1))
            self.content_length_check_result = self.content_block.length == expected
            logging.debug(f"Record content length check: found {self.content_block.length}, expected {expected}.")
        else:
            self.content_length_check_result = False

    def get_http_header_block(self):
        if (
            record_content_type_filter('http')(self) and
            self.content_block.bytes.find(CRLF*2)
        ):
            return self.content_block.bytes.split(CRLF*2)[0]

    def get_http_body_block(self):
        if (
            record_content_type_filter('http')(self) and
            self.content_block.bytes.find(CRLF*2)
        ):
            parts = self.content_block.bytes.split(CRLF*2)
            if len(parts) == 2:
                return parts[1]


@dataclass
class Header(ByteRange):
    pass


@dataclass
class ContentBlock(ByteRange):
    pass


#
# Record Filters
#

def warc_named_field_filter(field_name, target, case_insensitive=True, exact_match=False):
    def f(record):
        match = find_pattern_in_bytes(
            get_warc_named_field_pattern(field_name),
            record.header.bytes,
            case_insensitive=case_insensitive
        )
        if match:
            extracted = match.group(1)
            return find_match_in_extracted_header(
                extracted,
                target,
                case_insensitive=case_insensitive,
                exact_match=exact_match
            )
        return False
    return f


def warc_header_regex_filter(regex, case_insensitive=True):
    def f(record):
        return bool(
            find_pattern_in_bytes(
                bytes(regex, "utf-8"),
                record.header.bytes,
                case_insensitive=case_insensitive
            )
        )
    return f


def record_content_length_filter(target_length, operand="eq"):
    allowed_operators = {
        'lt': operator.lt,
        'le': operator.le,
        'eq': operator.eq,
        'ne': operator.ne,
        'gt': operator.gt,
        'ge': operator.ge,
    }
    if operand not in allowed_operators:
        raise ValueError(f"Supported operands: {', '.join(allowed_operators)}.")

    def f(record):
        match = find_pattern_in_bytes(CONTENT_LENGTH_PATTERN, record.header.bytes, case_insensitive=True)

        if match:
            extracted = int(match.group(1))
            return allowed_operators[operand](extracted, target_length)
        else:
            return False
    return f


def record_content_type_filter(content_type, case_insensitive=True, exact_match=False):
    """
    Filters on the Content-Type field of the WARC header.

    Expected values:
    - application/warc-fields
    - application/http; msgtype=request
    - application/http; msgtype=response
    - image/jpeg or another mime type, for resource records

    NB: This field does NOT refer to the content-type header of recorded HTTP responses.
    See `http_response_content_type_filter`.
    """
    def f(record):
        match = find_pattern_in_bytes(
            CONTENT_TYPE_PATTERN,
            record.header.bytes,
            case_insensitive=case_insensitive
        )
        if match:
            extracted = match.group(1)
            return find_match_in_extracted_header(
                extracted,
                content_type,
                case_insensitive=case_insensitive,
                exact_match=exact_match
            )
        return False
    return f


def http_verb_filter(verb):
    """
    Finds WARC records with a Content-Type of application/http; msgtype=request,
    then filters on HTTP verb.
    """
    def f(record):
        if record_content_type_filter('application/http; msgtype=request')(record):
            http_headers = record.get_http_header_block()
            match = find_pattern_in_bytes(
                get_http_verb_pattern(verb),
                http_headers
            )
            if match:
                extracted = match.group(1)
                return find_match_in_extracted_header(
                    extracted,
                    verb,
                    exact_match=True
                )
        return False
    return f


def http_status_filter(status_code):
    """
    Finds WARC records with a Content-Type of application/http; msgtype=response,
    then filters on HTTP status code.
    """
    def f(record):
        if record_content_type_filter('application/http; msgtype=response')(record):
            http_headers = record.get_http_header_block()
            match = find_pattern_in_bytes(
                get_http_status_pattern(status_code),
                http_headers
            )
            if match:
                extracted = match.group(1)
                return find_match_in_extracted_header(
                    extracted,
                    str(status_code),
                    exact_match=True
                )
        return False
    return f


def http_header_filter(header_name, target, case_insensitive=True, exact_match=False):
    """
    Finds WARC records with a Content-Type that includes application/http,
    then filters on any HTTP header.
    """
    def f(record):
        if record_content_type_filter('application/http')(record):
            http_headers = record.get_http_header_block()
            match = find_pattern_in_bytes(
                get_http_header_pattern(header_name),
                http_headers,
                case_insensitive=case_insensitive
            )
            if match:
                extracted = match.group(1)
                return find_match_in_extracted_header(
                    extracted,
                    target,
                    case_insensitive=case_insensitive,
                    exact_match=exact_match
                )
        return False
    return f


def http_response_content_type_filter(content_type, case_insensitive=True, exact_match=False):
    """
    Finds WARC records with a Content-Type of application/http; msgtype=response,
    then filters on the HTTP header "Content-Type".
    """
    def f(record):
        if record_content_type_filter('application/http; msgtype=response')(record):
            http_headers = record.get_http_header_block()
            match = find_pattern_in_bytes(
                CONTENT_TYPE_PATTERN,
                http_headers,
                case_insensitive=case_insensitive
            )
            if match:
                extracted = match.group(1)
                return find_match_in_extracted_header(
                    extracted,
                    content_type,
                    case_insensitive=case_insensitive,
                    exact_match=exact_match
                )
        return False
    return f


#
# Utils
#

class AttributeNotInitializedError(Exception):
    """Custom exception raised when trying to access an uninitialized attribute."""
    pass


def skip_leading_whitespace(file_handle):
    while True:
        byte = file_handle.read(1)
        if not byte.isspace():
            # Skip the cursor back one byte, so this non-whitespace
            # byte is isn't skipped
            file_handle.seek(-1, whence=os.SEEK_CUR)
            break
        else:
            logging.debug("Skipping whitespace!\n")


def preserve_cursor_position(func):
    @functools.wraps(func)
    def wrapper(file_handle, *args, **kwargs):
        # Save the original position of the file handle
        original_position = file_handle.tell()
        try:
            # Call the decorated function
            return func(file_handle, *args, **kwargs)
        finally:
            # Reset the file handle to the original position
            file_handle.seek(original_position)
    return wrapper


@preserve_cursor_position
def find_record_end(file_handle):
    last_line_had_a_break = False
    last_line_was_a_break = False

    while True:
        line = file_handle.readline()

        if not line:
            return None  # End of file reached without a record delimiter

        if line.endswith(CRLF):

            # We are only at a record end if this line is just a break.
            if line == CRLF:

                if last_line_was_a_break:
                    # We've found the delimiter! We might be done.
                    # Make sure there aren't more instance of \r\n to consume,
                    # lest we signal we've found the end of the record prematurely.
                    if not file_handle.peek(2).startswith(CRLF):
                        return file_handle.tell()  # End of record found

                if last_line_had_a_break:
                    # We've found the delimiter! We might be done.
                    # If the next line begins with "WARC", then we've found
                    # the end of this record and the start of the next one.
                    # (Expect this after content blocks with binary payloads.)
                    # Otherwise, we're still in the middle of a record.
                    if file_handle.peek(len(WARC_VERSION)).startswith(WARC_VERSION):
                        # TODO: in rare cases, I bet this catches false positives.
                        # For instance, what if the content block's payload is an
                        # HTML page with code blocks about WARC contents? :-)
                        return file_handle.tell()  # End of record found

                last_line_was_a_break = True

            else:
                last_line_was_a_break = False
                last_line_had_a_break = True
        else:
            last_line_was_a_break = False
            last_line_had_a_break = False

    return end_position


def split_record(
    record,
    record_bytes,
    cache_header_bytes,
    cache_content_block_bytes,
    enable_lazy_loading_of_bytes
):
    header_start = record.start
    header_end_index = record_bytes.find(CRLF*2)
    header_end = header_start + header_end_index

    content_block_start_index = header_end_index + len(CRLF*2)
    content_block_start = record.start + content_block_start_index
    content_block_end = record.end

    record.header = Header(
        start=header_start,
        end=header_end
    )
    if cache_header_bytes:
        record.header._bytes = record_bytes[:header_end_index]
    if enable_lazy_loading_of_bytes:
        record.header._file_handle = record._file_handle

    record.content_block = ContentBlock(
        start=content_block_start,
        end=content_block_end,
    )
    if cache_content_block_bytes:
        record.content_block._bytes = record_bytes[content_block_start_index:]
    if enable_lazy_loading_of_bytes:
        record.content_block._file_handle = record._file_handle


def find_pattern_in_bytes(pattern, data, case_insensitive=True):
    return re.search(pattern, data, re.IGNORECASE if case_insensitive else 0)


def find_match_in_extracted_header(
    extracted,
    target,
    case_insensitive=True,
    exact_match=False
):
    extracted_bytes = extracted.lower() if case_insensitive else extracted
    target_string = target.lower() if case_insensitive else target
    target_bytes = bytes(target_string, 'utf-8')

    if exact_match:
        return target_bytes == extracted_bytes
    return target_bytes in extracted_bytes


#
# Parser
#


STATES = {
    'FIND_HEADER': 'find_header',
    'EXTRACT_HEADER': 'extract_header',
    'FIND_NEXT_RECORD': 'find_next_record',
    'EXTRACT_NEXT_RECORD': 'extract_next_record',
    'END': 'end'
}

class WARCParser:
    def __init__(
        self,
        file_handle,
        check_content_lengths=False,
        cache_record_bytes=False,
        cache_header_bytes=False,
        cache_content_block_bytes=False,
        cache_unparsable_line_bytes=False,
        enable_lazy_loading_of_bytes=True
    ):
        # Validate Options
        if check_content_lengths:
            if not enable_lazy_loading_of_bytes and not all([
                cache_header_bytes,
                cache_content_block_bytes
            ]):
                raise ValueError(
                    "To check_content_lengths, you must either enable_lazy_loading_of_bytes or "
                    "both cache_header_bytes and cache_content_block_bytes."
                )

        #
        # Set Up
        #

        self.state = STATES['FIND_HEADER']
        self.transitions = {
            STATES['FIND_HEADER']: self.find_warc_header,
            STATES['EXTRACT_HEADER']: self.extract_warc_header,
            STATES['FIND_NEXT_RECORD']: self.find_next_record,
            STATES['EXTRACT_NEXT_RECORD']: self.extract_next_record,
            STATES['END']: None
        }

        self.file_handle = file_handle
        self.check_content_lengths = check_content_lengths
        self.cache_record_bytes=cache_record_bytes
        self.cache_header_bytes=cache_header_bytes
        self.cache_content_block_bytes=cache_content_block_bytes
        self.cache_unparsable_line_bytes=cache_unparsable_line_bytes
        self.enable_lazy_loading_of_bytes=enable_lazy_loading_of_bytes

        self.unparsable_lines = []
        self.warnings = []
        self.error = None
        self._records = None
        self.current_record = None


    def parse(self,
        filters=None,
        find_first_record_only=False
    ):
        self._records = []
        iterator = self.iterator(
            filters=filters,
            find_first_record_only=find_first_record_only
        )
        for record in iterator:
            self._records.append(record)

    @property
    def records(self):
        if self._records is None:
            raise AttributeNotInitializedError(
                "Call parser.parse() to load records into RAM and populate parser.records, "
                "or use parser.iterator() to iterate through records without preloading."
            )
        return self._records

    def iterator(self,
        filters=None,
        find_first_record_only=False
    ):
        self.file_handle.seek(0)

        while self.state != STATES['END']:
            self.current_record = None

            transition_func = self.transitions[self.state]
            self.state = transition_func()

            if self.current_record:
                if filters:
                    retained = True
                    for f in filters:
                        if not f(self.current_record):
                            retained = False
                            logging.debug(f"Skipping record at {self.current_record.start}-{self.current_record.end} due to filter.")
                            break

                    if retained:
                        yield self.current_record
                else:
                    yield self.current_record

                if find_first_record_only:
                    self.state = STATES['END']

        self.current_record = None

    def find_warc_header(self):
        skip_leading_whitespace(self.file_handle)
        initial_position = self.file_handle.tell()
        first_line = self.file_handle.readline()
        self.file_handle.seek(initial_position)

        if first_line == WARC_VERSION:
            return STATES['EXTRACT_HEADER']
        else:
            self.error = 'No WARC header found.'
            return STATES['END']

    def find_next_record(self):
        while True:
            initial_position = self.file_handle.tell()
            if self.file_handle.peek(len(WARC_VERSION)).startswith(WARC_VERSION):
                return STATES['EXTRACT_NEXT_RECORD']

            next_line = self.file_handle.readline()
            current_position = self.file_handle.tell()
            if next_line:
                unparsable_line = UnparsableLine(
                    start=initial_position,
                    end=current_position,
                )
                if self.cache_unparsable_line_bytes:
                    unparsable_line._bytes=next_line
                if self.enable_lazy_loading_of_bytes:
                    unparsable_line._file_handle=self.file_handle
                # TODO: if there are a large number of unparsable lines,
                # this could eat up RAM. Is there a better solution,
                # especially in iterator mode? We could just log,
                # or could make behavior configurable. Maybe, log in
                # iterator mode, store if in parser mode?
                self.unparsable_lines.append(unparsable_line)
            else:
                return STATES['END']

    def extract_next_record(self):
        start = self.file_handle.tell()
        stop = find_record_end(self.file_handle)
        if stop:
            # Don't include the delimiter in the record's data or offsets
            end = stop - len(CRLF*2)
            data = self.file_handle.read(end - start)
            # Advance the cursor past the delimiter
            self.file_handle.read(len(CRLF*2))
        else:
            self.warnings.append('Last record may have been truncated.')
            data = self.file_handle.read()
            end = self.file_handle.tell()

        record = Record(
            start=start,
            end=end
        )
        if self.cache_record_bytes:
            record._bytes = data
        if self.enable_lazy_loading_of_bytes:
            record._file_handle = self.file_handle
        split_record(
            record,
            data,
            cache_header_bytes=self.cache_header_bytes,
            cache_content_block_bytes=self.cache_content_block_bytes,
            enable_lazy_loading_of_bytes=self.enable_lazy_loading_of_bytes
        )

        if self.check_content_lengths:
            record.check_content_length()
        self.current_record = record

        if end:
            return STATES['FIND_NEXT_RECORD']
        return STATES['END']

    def extract_warc_header(self):
        self.extract_next_record()
        return STATES['FIND_NEXT_RECORD']


with open("579F-LLZR.wacz", "rb") as wacz_file, \
    zipfile.Path(wacz_file, "archive/data.warc.gz").open("rb") as warc_gz_file, \
    gzip.open(warc_gz_file, "rb") as warc_file:
        parser = WARCParser(
            warc_file,
            check_content_lengths=True,
            # cache_record_bytes=True,
            # cache_header_bytes=True,
            # cache_content_block_bytes=True,
            # cache_unparsable_line_bytes=True,
            enable_lazy_loading_of_bytes=True
        )
        parser.parse(
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
            # find_first_record_only=True,
        )
        print(len(parser.records))
        # for record in parser.records:
            # print(record.get_http_header_block())
            # print(record.get_http_body_block())
            # print("\n\n")
