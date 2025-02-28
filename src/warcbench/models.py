"""
`models` module: Dataclasses for storing parsed WARC pieces
"""

from abc import ABC
from dataclasses import dataclass, field
import io
import logging
from typing import Optional

from warcbench.patterns import CRLF, CONTENT_LENGTH_PATTERN
from warcbench.utils import find_pattern_in_bytes
from warcbench.filters import record_content_type_filter

logger = logging.getLogger(__name__)


@dataclass
class ByteRange(ABC):
    """
    The base class from which all others inherit.
    Records the starting and ending offsets of a range of bytes in a file,
    and provides utilities for interacting with those bytes.
    """

    start: int
    end: int
    _bytes: Optional[bytes] = field(repr=False, default=None)
    _file_handle: Optional[io.BufferedReader] = field(repr=False, default=None)

    def __post_init__(self):
        self.length = self.end - self.start

    @property
    def bytes(self):
        """
        Load all the bytes into memory and return them as a bytestring.
        """
        if self._bytes is None:
            data = bytearray()
            for chunk in self.iterator():
                data.extend(chunk)
            return bytes(data)
        return self._bytes

    def iterator(self, chunk_size=1024):
        """
        Returns an iterator that yields the bytes in chunks.
        """
        if self._bytes:
            for i in range(0, len(self._bytes), chunk_size):
                yield self._bytes[i : i + chunk_size]

        else:
            if not self._file_handle:
                raise ValueError(
                    "To access record bytes, you must either enable_lazy_loading_of_bytes or "
                    "cache_record_bytes/cache_header_bytes/cache_content_block_bytes."
                )

            logger.debug(f"Reading from {self.start} to {self.end}.")

            original_postion = self._file_handle.tell()

            self._file_handle.seek(self.start)
            while self._file_handle.tell() < self.end:
                # Calculate the remaining bytes to read
                remaining_bytes = self.end - self._file_handle.tell()

                # Determine the actual chunk size to read
                actual_chunk_size = min(chunk_size, remaining_bytes)

                yield self._file_handle.read(actual_chunk_size)

            self._file_handle.seek(original_postion)


@dataclass
class Record(ByteRange):
    """
    A WARC record
    http://iipc.github.io/warc-specifications/specifications/warc-format/warc-1.1/#warc-record

    Comprises a WARC record header and a WARC record content block.
    """

    content_length_check_result: Optional[int] = None

    def split(
        self,
        record_bytes,
        cache_header_bytes,
        cache_content_block_bytes,
        enable_lazy_loading_of_bytes,
    ):
        header_start = self.start
        header_end_index = record_bytes.find(CRLF * 2)
        header_end = header_start + header_end_index

        content_block_start_index = header_end_index + len(CRLF * 2)
        content_block_start = self.start + content_block_start_index
        content_block_end = self.end

        self.header = Header(start=header_start, end=header_end)
        if cache_header_bytes:
            self.header._bytes = record_bytes[:header_end_index]
        if enable_lazy_loading_of_bytes:
            self.header._file_handle = self._file_handle

        self.content_block = ContentBlock(
            start=content_block_start,
            end=content_block_end,
        )
        if cache_content_block_bytes:
            self.content_block._bytes = record_bytes[content_block_start_index:]
        if enable_lazy_loading_of_bytes:
            self.content_block._file_handle = self._file_handle

    def check_content_length(self):
        """
        Valid WARC record headers include a Content-Length field that specifies the number of bytes
        in the record's content block.
        http://iipc.github.io/warc-specifications/specifications/warc-format/warc-1.1/#content-length-mandatory

        Search for the content length in the header, and compare it against the number of bytes
        detected when the WARC file was parsed.
        """
        match = find_pattern_in_bytes(CONTENT_LENGTH_PATTERN, self.header.bytes)

        if match:
            expected = int(match.group(1))
            self.content_length_check_result = self.content_block.length == expected
            logger.debug(
                f"Record content length check: found {self.content_block.length}, expected {expected}."
            )
        else:
            self.content_length_check_result = False

    def get_http_header_block(self):
        """
        If this WARC record describes an HTTP exchange, extract the HTTP headers of that exchange.
        """
        # We expect WARC records that describe HTTP exchanges to have a Content-Type that contains "application/http".
        # http://iipc.github.io/warc-specifications/specifications/warc-format/warc-1.1/#content-type
        if record_content_type_filter("http")(self) and self.content_block.bytes.find(
            CRLF * 2
        ):
            return self.content_block.bytes.split(CRLF * 2)[0]

    def get_http_body_block(self):
        """
        If this WARC record describes an HTTP exchange, extract the HTTP body of that exchange (if any).
        """
        # We expect WARC records that describe HTTP exchanges to have a Content-Type that contains "application/http".
        # http://iipc.github.io/warc-specifications/specifications/warc-format/warc-1.1/#content-type
        if record_content_type_filter("http")(self) and self.content_block.bytes.find(
            CRLF * 2
        ):
            parts = self.content_block.bytes.split(CRLF * 2)
            if len(parts) == 2:
                return parts[1]


@dataclass
class Header(ByteRange):
    """
    A WARC record header
    http://iipc.github.io/warc-specifications/specifications/warc-format/warc-1.1/#warc-record-header
    """

    pass


@dataclass
class ContentBlock(ByteRange):
    """
    A WARC record content block
    http://iipc.github.io/warc-specifications/specifications/warc-format/warc-1.1/#warc-record-content-block
    """

    pass


@dataclass
class UnparsableLine(ByteRange):
    """
    Any line that was unexpected, during parsing.
    Unparsable lines are not included in any WARC records detected while parsing.
    """

    pass
