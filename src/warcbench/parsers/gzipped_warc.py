"""
`parsers.gzipped_warc` module: Classes that slice a gzipped WARC into pieces, using different strategies
"""

import logging
import os
from tempfile import NamedTemporaryFile

from warcbench.exceptions import AttributeNotInitializedError, DecompressionError
from warcbench.models import (
    GzippedMember,
    UncompressedGzipData,
    Record,
    Header,
    ContentBlock,
)
from warcbench.patches import patched_gzip
from warcbench.patterns import CRLF, WARC_VERSIONS
from warcbench.utils import (
    find_next_header_end,
    find_content_length_in_bytes,
    get_gzip_file_member_offsets,
)

logger = logging.getLogger(__name__)

STATES = {
    "LOCATE_MEMBERS": "locate_members",
    "FIND_NEXT_MEMBER": "find_next_member",
    "EXTRACT_NEXT_MEMBER": "extract_next_member",
    "CHECK_MEMBER_AGAINST_FILTERS": "check_member_against_filters",
    "RUN_MEMBER_HANDLERS": "run_member_handlers",
    "YIELD_CURRENT_MEMBER": "yield_member",
    "RUN_PARSER_CALLBACKS": "run_parser_callbacks",
    "END": "end",
}


class GzippedWARCMemberParser:
    warnings = []
    error = None
    current_member = None
    current_offsets = None

    _offsets = None
    _members = None

    def __init__(
        self,
        file_handle,
        stop_after_nth,
        decompress_and_parse_members,
        decompress_chunk_size,
        cache_non_warc_member_bytes,
        filters,
        member_handlers,
        record_handlers,
        non_warc_member_handlers,
        parser_callbacks,
    ):
        #
        # Validate Options
        #
        # if not decompress_and_parse_members:
        #     if (
        #         split_records
        #         or cache_non_warc_members
        #         or cache_record_bytes
        #         or cache_header_bytes
        #         or cache_content_block_bytes
        #         or cache_non_warc_members_bytes
        #         or non_warc_member_handlers
        #     ):
        #         raise ValueError(
        #             "You must enable the decompression of members to further parse their contents."
        #         )

        # if cache_header_bytes or cache_content_block_bytes:
        #     if not split_records:
        #         raise ValueError(
        #             "To cache header or content block bytes, you must split records."
        #         )

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
            STATES["RUN_PARSER_CALLBACKS"]: self.run_parser_callbacks,
            STATES["END"]: None,
        }

        self.file_handle = file_handle
        self.stop_after_nth = stop_after_nth
        self.decompress_and_parse_members = decompress_and_parse_members
        self.decompress_chunk_size = decompress_chunk_size
        self.cache_non_warc_member_bytes = cache_non_warc_member_bytes
        self.filters = filters
        self.member_handlers = member_handlers
        self.non_warc_member_handlers = non_warc_member_handlers
        self.parser_callbacks = parser_callbacks

        if cache_non_warc_member_bytes:
            self._non_warc_members = []
        else:
            self._non_warc_members = None

    @property
    def members(self):
        if self._members is None:
            raise AttributeNotInitializedError(
                "Call parser.parse() to load members into RAM and populate parser.members, "
                "or use parser.iterator() to iterate through members without preloading."
            )
        return self._members

    @property
    def records(self):
        if self._members is None:
            raise AttributeNotInitializedError(
                "Call parser.parse() to load records into RAM and populate parser.records, "
                "or use parser.iterator(yield_type='records') to iterate through successfully "
                "parsed records without preloading."
            )
        return [member.record for member in self._members if member.record]

    @property
    def non_warc_members(self):
        if self._non_warc_members is None:
            raise AttributeNotInitializedError(
                "Pass cache_non_warc_members=True to WARCGZParser() to store NonWARCMembers "
                "in parser.unparsable_lines."
            )
        return self._non_warc_members

    def parse(self):
        self._members = []
        iterator = self.iterator()
        for member in iterator:
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
                    if self.current_member.record:
                        yielded = yielded + 1
                        yield self.current_member.record
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

    def locate_members(self):
        """
        Read through the entire gzip file and locate the boundaries of its members.
        """
        self._offsets = get_gzip_file_member_offsets(self.file_handle)
        self.file_handle.seek(0)
        if len(self._offsets) == 1:
            self.warnings.append(
                "This file may not be composed of separately gzipped WARC records: only one gzip member found."
            )
        return STATES["FIND_NEXT_MEMBER"]

    def find_next_member(self):
        try:
            self.current_offsets = self._offsets.popleft()
            return STATES["EXTRACT_NEXT_MEMBER"]
        except IndexError:
            return STATES["RUN_PARSER_CALLBACKS"]

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

        #
        # Gunzip for further parsing
        #

        if self.decompress_and_parse_members:
            with NamedTemporaryFile("w+b", delete=False) as extracted_member_file:
                extracted_member_file_name = extracted_member_file.name

                self.file_handle.seek(member.start)

                # Read the member data in chunks and write to the temp file
                bytes_read = 0
                while bytes_read < member.length:
                    to_read = min(
                        self.decompress_chunk_size, member.length - bytes_read
                    )
                    chunk = self.file_handle.read(to_read)
                    if not chunk:
                        raise DecompressionError(
                            f"Invalid offsets for member reported at {member.start} - {member.end}."
                        )  # End of file reached unexpectedly
                    extracted_member_file.write(chunk)
                    bytes_read += len(chunk)

                extracted_member_file.flush()

            with patched_gzip.open(extracted_member_file_name, "rb") as gunzipped_file:
                header_start = 0
                header_with_linebreak_end = find_next_header_end(
                    gunzipped_file, self.decompress_chunk_size
                )
                if header_with_linebreak_end:
                    # Don't include the line break in the header's data or offsets
                    header_end = header_with_linebreak_end - len(CRLF)
                    header_bytes = gunzipped_file.read(header_end - header_start)
                    gunzipped_file.read(len(CRLF))
                else:
                    header_bytes = gunzipped_file.read()
                    header_end = gunzipped_file.tell()

                #
                # Build the Record object
                #

                # See if this claims to be a WARC header
                header_found = False
                for warc_version in WARC_VERSIONS:
                    if header_bytes.startswith(warc_version):
                        header_found = True
                        break

                # Extract the value of the mandatory Content-Length field
                content_length = find_content_length_in_bytes(header_bytes)

                if not header_found or not content_length:
                    # This member isn't parsable as a WARC record
                    if self.cache_non_warc_member_bytes:
                        gunzipped_file.seek(0)
                        member.uncompressed_non_warc_data = UncompressedGzipData(
                            bytes=gunzipped_file.read()
                        )

                else:
                    content_start = gunzipped_file.tell()
                    content_bytes = gunzipped_file.read(content_length)
                    content_end = gunzipped_file.tell()

                    record = Record(start=start, end=end)
                    data = bytearray()
                    data.extend(header_bytes)
                    data.extend(b"\n")
                    data.extend(content_bytes)
                    record._bytes = bytes(data)

                    header = Header(start=header_start, end=header_end)
                    header._bytes = header_bytes

                    content_block = ContentBlock(start=content_start, end=content_end)
                    content_block._bytes = content_bytes

                    record.header = header
                    record.content_block = content_block

                    member.uncompressed_warc_record = record

            os.remove(extracted_member_file_name)

        return STATES["CHECK_MEMBER_AGAINST_FILTERS"]

    def check_member_against_filters(self):
        retained = True
        if self.filters:
            for f in self.filters:
                if not f(self.current_member):
                    retained = False
                    logger.debug(
                        f"Skipping member at {self.current_member.start}-{self.current_member.end} due to filter."
                    )
                    break

        if retained:
            return STATES["RUN_MEMBER_HANDLERS"]
        return STATES["FIND_NEXT_MEMBER"]

    def run_member_handlers(self):
        if self.member_handlers:
            for f in self.member_handlers:
                f(self.current_member)

        return STATES["YIELD_CURRENT_MEMBER"]

    def run_parser_callbacks(self):
        if self.parser_callbacks:
            for f in self.parser_callbacks:
                f(self)

        return STATES["END"]
