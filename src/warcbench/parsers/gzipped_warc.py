"""
`parsers.gzipped_warc` module: Classes that slice a gzipped WARC into pieces, using different strategies
"""

from abc import ABC, abstractmethod
import logging
import os
from tempfile import NamedTemporaryFile

from warcbench.exceptions import AttributeNotInitializedError, DecompressionError
from warcbench.models import (
    GzippedMember,
    Record,
    Header,
    ContentBlock,
)
from warcbench.patches import patched_gzip
from warcbench.patterns import CRLF, WARC_VERSIONS
from warcbench.utils import (
    find_next_header_end,
    find_content_length_in_bytes,
    decompress_and_get_gzip_file_member_offsets,
)

logger = logging.getLogger(__name__)

STATES = {
    "LOCATE_MEMBERS": "locate_members",
    "FIND_NEXT_MEMBER": "find_next_member",
    "EXTRACT_NEXT_MEMBER": "extract_next_member",
    "CHECK_MEMBER_AGAINST_FILTERS": "check_member_against_filters",
    "RUN_MEMBER_HANDLERS": "run_member_handlers",
    "RUN_RECORD_HANDLERS": "run_record_handlers",
    "YIELD_CURRENT_MEMBER": "yield_member",
    "RUN_PARSER_CALLBACKS": "run_parser_callbacks",
    "END": "end",
}


class BaseParser(ABC):
    def __init__(
        self,
        file_handle,
        stop_after_nth,
        decompress_chunk_size,
        split_records,
        cache_member_bytes,
        cache_member_uncompressed_bytes,
        cache_record_bytes,
        cache_header_bytes,
        cache_parsed_headers,
        cache_content_block_bytes,
        cache_non_warc_member_bytes,
        member_filters,
        record_filters,
        member_handlers,
        record_handlers,
        parser_callbacks,
    ):
        #
        # Validate Options
        #

        if cache_header_bytes or cache_parsed_headers or cache_content_block_bytes:
            if not split_records:
                raise ValueError(
                    "To cache or parse header or content block bytes, you must split records."
                )

        #
        # Set Up
        #

        self.state = STATES["LOCATE_MEMBERS"]
        self.transitions = {
            STATES["LOCATE_MEMBERS"]: self.locate_members,
            STATES["FIND_NEXT_MEMBER"]: self.find_next_member,
            STATES["EXTRACT_NEXT_MEMBER"]: self.extract_next_member,
            STATES["CHECK_MEMBER_AGAINST_FILTERS"]: self.check_member_against_filters,
            STATES["RUN_MEMBER_HANDLERS"]: self.run_member_handlers,
            STATES["RUN_RECORD_HANDLERS"]: self.run_record_handlers,
            STATES["RUN_PARSER_CALLBACKS"]: self.run_parser_callbacks,
            STATES["END"]: None,
        }

        self.file_handle = file_handle
        self.stop_after_nth = stop_after_nth
        self.decompress_chunk_size = decompress_chunk_size
        self.split_records = split_records
        self.cache_member_bytes = cache_member_bytes
        self.cache_member_uncompressed_bytes = cache_member_uncompressed_bytes
        self.cache_record_bytes = cache_record_bytes
        self.cache_header_bytes = cache_header_bytes
        self.cache_parsed_headers = cache_parsed_headers
        self.cache_content_block_bytes = cache_content_block_bytes
        self.cache_non_warc_member_bytes = cache_non_warc_member_bytes
        self.member_filters = member_filters
        self.record_filters = record_filters
        self.member_handlers = member_handlers
        self.record_handlers = record_handlers
        self.parser_callbacks = parser_callbacks

        self.warnings = []
        self.error = None
        self.current_member = None
        self.current_offsets = None

        self._offsets = None
        self._members = None

    @property
    def members(self):
        if self._members is None:
            raise AttributeNotInitializedError(
                "Call parser.parse(cache_members=True) to load members into RAM and populate parser.members, "
                "or use parser.iterator() to iterate through members without preloading."
            )
        return self._members

    @property
    def records(self):
        if self._members is None:
            raise AttributeNotInitializedError(
                "Call parser.parse(cache_members=True) to load records into RAM and populate parser.records, "
                "or use parser.iterator(yield_type='records') to iterate through successfully "
                "parsed records without preloading."
            )
        return [
            member.uncompressed_warc_record
            for member in self._members
            if member.uncompressed_warc_record
        ]

    def parse(self, cache_members):
        iterator = self.iterator()
        if cache_members:
            self._members = []
        for member in iterator:
            if cache_members:
                self._members.append(member)

    def iterator(self, yield_type="members"):
        yielded = 0
        self.file_handle.seek(0)

        while self.state != STATES["END"]:
            if self.state == STATES["YIELD_CURRENT_MEMBER"]:
                if yield_type == "members":
                    yielded = yielded + 1
                    yield self.current_member
                elif yield_type == "records":
                    if self.current_member.uncompressed_warc_record:
                        yielded = yielded + 1
                        yield self.current_member.uncompressed_warc_record
                    else:
                        logger.debug(
                            f"Skipping member at {self.current_member.start}-{self.current_member.end} because no WARC record was found."
                        )
                self.current_member = None

                if self.stop_after_nth and yielded >= self.stop_after_nth:
                    logger.debug(
                        f"Stopping early after yielding {self.stop_after_nth} members."
                    )
                    self.state = STATES["RUN_PARSER_CALLBACKS"]
                    continue

                self.state = STATES["FIND_NEXT_MEMBER"]
            else:
                transition_func = self.transitions[self.state]
                self.state = transition_func()

    def get_member_offsets(self, compressed):
        members = self._members if self._members else self.iterator()
        if compressed:
            return [(member.start, member.end) for member in members]
        return [
            (member.uncompressed_start, member.uncompressed_end) for member in members
        ]

    def get_record_offsets(self, split):
        records = (
            self.records() if self._members else self.iterator(yield_type="records")
        )

        if split:
            if not self.split_records:
                raise ValueError(
                    "Split record offsets are only available when the parser is initialized with split_records=True."
                )
            return [
                (
                    record.header.start,
                    record.header.end,
                    record.content_block.start,
                    record.content_block.end,
                )
                for record in records
            ]

        return [(record.start, record.end) for record in records]

    #
    # Internal methods
    #

    def find_next_member(self):
        try:
            self.current_offsets = self._offsets.popleft()
            return STATES["EXTRACT_NEXT_MEMBER"]
        except IndexError:
            return STATES["RUN_PARSER_CALLBACKS"]

    def check_member_against_filters(self):
        retained = True

        if self.member_filters:
            for f in self.member_filters:
                if not f(self.current_member):
                    retained = False
                    logger.debug(
                        f"Skipping member at {self.current_member.start}-{self.current_member.end} due to member filter."
                    )
                    break

        if self.record_filters:
            for f in self.record_filters:
                if not self.current_member.uncompressed_warc_record or not f(
                    self.current_member.uncompressed_warc_record
                ):
                    retained = False
                    logger.debug(
                        f"Skipping member at {self.current_member.start}-{self.current_member.end} due to record filter."
                    )
                    break

        if retained:
            return STATES["RUN_MEMBER_HANDLERS"]
        return STATES["FIND_NEXT_MEMBER"]

    def run_member_handlers(self):
        if self.member_handlers:
            for f in self.member_handlers:
                f(self.current_member)

        return STATES["RUN_RECORD_HANDLERS"]

    def run_record_handlers(self):
        if self.record_handlers and self.current_member.uncompressed_warc_record:
            for f in self.record_handlers:
                f(self.current_member.uncompressed_warc_record)

        return STATES["YIELD_CURRENT_MEMBER"]

    def run_parser_callbacks(self):
        if self.parser_callbacks:
            for f in self.parser_callbacks:
                f(self)

        return STATES["END"]

    @abstractmethod
    def locate_members(self):
        pass

    @abstractmethod
    def extract_next_member(self):
        pass


class GzippedWARCMemberParser(BaseParser):
    def __init__(
        self,
        file_handle,
        stop_after_nth,
        decompress_and_parse_members,
        decompress_chunk_size,
        split_records,
        cache_member_bytes,
        cache_member_uncompressed_bytes,
        cache_record_bytes,
        cache_header_bytes,
        cache_parsed_headers,
        cache_content_block_bytes,
        cache_non_warc_member_bytes,
        member_filters,
        record_filters,
        member_handlers,
        record_handlers,
        parser_callbacks,
    ):
        #
        # Validate options
        #

        if (
            split_records
            or cache_record_bytes
            or cache_header_bytes
            or cache_parsed_headers
            or cache_content_block_bytes
            or cache_non_warc_member_bytes
            or cache_member_uncompressed_bytes
        ):
            if not decompress_and_parse_members:
                raise ValueError(
                    "You must enable the decompression of members to further parse their contents."
                )

        #
        # Set up
        #

        super().__init__(
            file_handle,
            stop_after_nth,
            decompress_chunk_size,
            split_records,
            cache_member_bytes,
            cache_member_uncompressed_bytes,
            cache_record_bytes,
            cache_header_bytes,
            cache_parsed_headers,
            cache_content_block_bytes,
            cache_non_warc_member_bytes,
            member_filters,
            record_filters,
            member_handlers,
            record_handlers,
            parser_callbacks,
        )
        self.decompress_and_parse_members = decompress_and_parse_members

    def get_record_offsets(self, split):
        if not self.decompress_and_parse_members:
            raise ValueError(
                "Record offsets are only available when the parser is initialized with decompress_and_parse_members=True."
            )
        return super().get_record_offsets(split)

    def locate_members(self):
        """
        Read through the entire gzip file and locate the boundaries of its members.
        """
        self._offsets = decompress_and_get_gzip_file_member_offsets(
            self.file_handle,
            chunk_size=self.decompress_chunk_size,
        )
        if len(self._offsets) == 1:
            self.warnings.append(
                "This file may not be composed of separately gzipped WARC records: only one gzip member found."
            )
        return STATES["FIND_NEXT_MEMBER"]

    def extract_next_member(self):
        #
        # The raw bytes of the still-gzipped record
        #

        start, end = self.current_offsets[0]
        uncompressed_start, uncompressed_end = self.current_offsets[1]
        member = GzippedMember(
            uncompressed_start=uncompressed_start,
            uncompressed_end=uncompressed_end,
            start=start,
            end=end,
        )
        self.current_member = member
        if self.cache_member_bytes:
            self.file_handle.seek(member.start)
            member._bytes = self.file_handle.read(member.length)

        #
        # Individually gunzip members for further parsing
        #

        if self.decompress_and_parse_members:
            extracted_member_file = NamedTemporaryFile("w+b", delete=False)
            extracted_member_file_name = extracted_member_file.name

            self.file_handle.seek(member.start)

            # Read the member data in chunks and write to the temp file
            bytes_read = 0
            while bytes_read < member.length:
                to_read = min(self.decompress_chunk_size, member.length - bytes_read)
                chunk = self.file_handle.read(to_read)
                if not chunk:
                    raise DecompressionError(
                        f"Invalid offsets for member reported at {member.start} - {member.end}."
                    )  # End of file reached unexpectedly
                extracted_member_file.write(chunk)
                bytes_read += len(chunk)

            extracted_member_file.flush()

            with patched_gzip.open(extracted_member_file_name, "rb") as gunzipped_file:
                if self.split_records:
                    # See if this claims to be a WARC record
                    header_found = False
                    for warc_version in WARC_VERSIONS:
                        if gunzipped_file.peek(len(warc_version)).startswith(
                            warc_version
                        ):
                            header_found = True
                            break

                    # Find the header
                    header_start = uncompressed_start
                    header_with_linebreak_end = find_next_header_end(
                        gunzipped_file, self.decompress_chunk_size
                    )
                    if header_with_linebreak_end:
                        # Don't include the line break in the header's data or offsets
                        header_end = (
                            uncompressed_start + header_with_linebreak_end - len(CRLF)
                        )
                        header_bytes = gunzipped_file.read(header_end - header_start)
                        gunzipped_file.read(len(CRLF))
                    else:
                        header_bytes = gunzipped_file.read()
                        header_end = header_start + gunzipped_file.tell()

                    # Extract the value of the mandatory Content-Length field
                    content_length = find_content_length_in_bytes(header_bytes)

                    if not header_found or not content_length:
                        # This member isn't parsable as a WARC record
                        if self.cache_non_warc_member_bytes:
                            gunzipped_file.seek(0)
                            member.uncompressed_non_warc_data = gunzipped_file.read()
                            self.warnings.append(
                                f"The member at {start}-{end}, when gunzipped, does not appear to be a WARC record."
                            )

                    else:
                        content_start = header_end + len(CRLF)
                        content_end = content_start + content_length
                        if self.cache_record_bytes or self.cache_content_block_bytes:
                            content_bytes = gunzipped_file.read(content_length)

                        # Build the Record object
                        record = Record(start=header_start, end=content_end)
                        if self.cache_record_bytes:
                            data = bytearray()
                            data.extend(header_bytes)
                            data.extend(b"\n")
                            data.extend(content_bytes)
                            record._bytes = bytes(data)

                        header = Header(start=header_start, end=header_end)
                        if self.cache_header_bytes:
                            header._bytes = header_bytes

                        if self.cache_parsed_headers:
                            header._parsed_fields = header.parse_bytes_into_fields(
                                header_bytes
                            )

                        content_block = ContentBlock(
                            start=content_start, end=content_end
                        )
                        if self.cache_content_block_bytes:
                            content_block._bytes = content_bytes

                        record.header = header
                        record.content_block = content_block

                        member.uncompressed_warc_record = record

                        if gunzipped_file.read() == CRLF * 2:
                            self.warnings.append(
                                f"The member at {start}-{end}, when gunzipped, does not end with the expected WARC delimiter."
                            )

                else:
                    gunzipped_file.seek(-(len(CRLF * 2)), os.SEEK_END)
                    record_length = gunzipped_file.tell()
                    suffix = gunzipped_file.read()

                    if suffix != CRLF * 2:
                        self.warnings.append(
                            f"The member at {start}-{end}, when gunzipped, does not end with the expected WARC delimiter."
                        )
                        record_length = record_length + len(CRLF * 2)

                    record = Record(
                        start=uncompressed_start, end=uncompressed_start + record_length
                    )

                    if self.cache_record_bytes:
                        gunzipped_file.seek(0)
                        record._bytes = gunzipped_file.read(record_length)

                    member.uncompressed_warc_record = record

                if self.cache_member_uncompressed_bytes:
                    gunzipped_file.seek(0)
                    member._uncompressed_bytes = gunzipped_file.read()

            os.remove(extracted_member_file_name)

        return STATES["CHECK_MEMBER_AGAINST_FILTERS"]


class GzippedWARCDecompressingParser(BaseParser):
    def __init__(
        self,
        file_handle,
        stop_after_nth,
        decompress_chunk_size,
        split_records,
        cache_member_bytes,
        cache_member_uncompressed_bytes,
        cache_record_bytes,
        cache_header_bytes,
        cache_parsed_headers,
        cache_content_block_bytes,
        cache_non_warc_member_bytes,
        enable_lazy_loading_of_bytes,
        member_filters,
        record_filters,
        member_handlers,
        record_handlers,
        parser_callbacks,
    ):
        super().__init__(
            file_handle,
            stop_after_nth,
            decompress_chunk_size,
            split_records,
            cache_member_bytes,
            cache_member_uncompressed_bytes,
            cache_record_bytes,
            cache_header_bytes,
            cache_parsed_headers,
            cache_content_block_bytes,
            cache_non_warc_member_bytes,
            member_filters,
            record_filters,
            member_handlers,
            record_handlers,
            parser_callbacks,
        )
        self.enable_lazy_loading_of_bytes = enable_lazy_loading_of_bytes
        self.uncompressed_file_handle = NamedTemporaryFile("w+b", delete=False)

    def iterator(self, *args, **kwargs):
        for member in super().iterator(*args, **kwargs):
            yield member
        os.remove(self.uncompressed_file_handle.name)

    def locate_members(self):
        """
        Read through the entire gzip file and locate the boundaries of its members.
        Store the uncompressed data in a tempfile, for further processing.
        """
        self._offsets = decompress_and_get_gzip_file_member_offsets(
            self.file_handle,
            self.uncompressed_file_handle,
            self.decompress_chunk_size,
        )
        if len(self._offsets) == 1:
            self.warnings.append(
                "This file may not be composed of separately gzipped WARC records: only one gzip member found."
            )
        return STATES["FIND_NEXT_MEMBER"]

    def extract_next_member(self):
        #
        # The raw bytes of the still-gzipped record
        #

        start, end = self.current_offsets[0]
        uncompressed_start, uncompressed_end = self.current_offsets[1]
        member = GzippedMember(
            uncompressed_start=uncompressed_start,
            uncompressed_end=uncompressed_end,
            start=start,
            end=end,
        )
        self.current_member = member
        if self.cache_member_bytes:
            self.file_handle.seek(member.start)
            member._bytes = self.file_handle.read(member.length)
        if self.cache_member_uncompressed_bytes:
            self.uncompressed_file_handle.seek(member.uncompressed_start)
            member._uncompressed_bytes = self.uncompressed_file_handle.read(
                member.uncompressed_length
            )
        if self.enable_lazy_loading_of_bytes:
            member._file_handle = self.file_handle
            member._uncompressed_file_handle = self.uncompressed_file_handle

        #
        # Further parse the gunzipped members
        #

        self.uncompressed_file_handle.seek(uncompressed_start)

        if self.split_records:
            # See if this claims to be a WARC record
            header_found = False
            for warc_version in WARC_VERSIONS:
                if self.uncompressed_file_handle.peek(len(warc_version)).startswith(
                    warc_version
                ):
                    header_found = True
                    break

            # Find the header
            header_start = uncompressed_start
            header_with_linebreak_end = find_next_header_end(
                self.uncompressed_file_handle, self.decompress_chunk_size
            )
            if header_with_linebreak_end:
                # Don't include the line break in the header's data or offsets
                header_end = header_with_linebreak_end - len(CRLF)
                header_bytes = self.uncompressed_file_handle.read(
                    header_end - header_start
                )
                self.uncompressed_file_handle.read(len(CRLF))
            else:
                header_bytes = self.uncompressed_file_handle.read()
                header_end = self.uncompressed_file_handle.tell()

            # Extract the value of the mandatory Content-Length field
            content_length = find_content_length_in_bytes(header_bytes)

            if not header_found or not content_length:
                # This member isn't parsable as a WARC record
                if self.cache_non_warc_member_bytes:
                    self.uncompressed_file_handle.seek(uncompressed_start)
                    member.uncompressed_non_warc_data = (
                        self.uncompressed_file_handle.read(member.uncompressed_length)
                    )
                    self.warnings.append(
                        f"The member at {start}-{end}, when gunzipped, does not appear to be a WARC record."
                    )

            else:
                content_start = header_end + len(CRLF)
                content_end = content_start + content_length
                if self.cache_record_bytes or self.cache_content_block_bytes:
                    content_bytes = self.uncompressed_file_handle.read(content_length)
                else:
                    self.uncompressed_file_handle.seek(content_length, os.SEEK_CUR)

                # Build the Record object
                record = Record(start=header_start, end=content_end)
                if self.cache_record_bytes:
                    data = bytearray()
                    data.extend(header_bytes)
                    data.extend(b"\n")
                    data.extend(content_bytes)
                    record._bytes = bytes(data)
                if self.enable_lazy_loading_of_bytes:
                    record._file_handle = self.uncompressed_file_handle

                header = Header(start=header_start, end=header_end)
                if self.cache_header_bytes:
                    header._bytes = header_bytes
                if self.enable_lazy_loading_of_bytes:
                    header._file_handle = self.uncompressed_file_handle
                if self.cache_parsed_headers:
                    header._parsed_fields = header.parse_bytes_into_fields(header_bytes)

                content_block = ContentBlock(start=content_start, end=content_end)
                if self.cache_content_block_bytes:
                    content_block._bytes = content_bytes
                if self.enable_lazy_loading_of_bytes:
                    content_block._file_handle = self.uncompressed_file_handle

                record.header = header
                record.content_block = content_block

                member.uncompressed_warc_record = record

                if not self.uncompressed_file_handle.peek(len(CRLF * 2)).startswith(
                    CRLF * 2
                ):
                    self.warnings.append(
                        f"The member at {start}-{end}, when gunzipped, does not end with the expected WARC delimiter."
                    )

        else:
            record_length = member.uncompressed_length - len(CRLF * 2)
            if self.cache_record_bytes:
                record_bytes = bytearray
                record_bytes.extend(self.uncompressed_file_handle.read(record_length))
            else:
                self.uncompressed_file_handle.read(record_length)
            suffix = self.uncompressed_file_handle.read(len(CRLF * 2))
            if suffix != CRLF * 2:
                self.warnings.append(
                    f"The member at {start}-{end}, when gunzipped, does not end with the expected WARC delimiter."
                )
                record_length = record_length + len(CRLF * 2)
                record_bytes.extend(CRLF * 2)

            record = Record(
                start=uncompressed_start, end=uncompressed_start + record_length
            )
            if self.cache_record_bytes:
                record._bytes = record_bytes

            member.uncompressed_warc_record = record

        return STATES["CHECK_MEMBER_AGAINST_FILTERS"]
