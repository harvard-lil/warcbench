"""
`models` module: Dataclasses for storing parsed WARC pieces
"""

from __future__ import annotations

# Standard library imports
from abc import ABC
import builtins
from collections import defaultdict
from dataclasses import dataclass, field
import logging

# Warcbench imports
from warcbench.exceptions import SplitRecordsRequiredError
from warcbench.filters import record_content_type_filter
from warcbench.patterns import CRLF, CONTENT_LENGTH_PATTERN
from warcbench.utils import (
    ArchiveFileHandle,
    concatenate_chunked_http_response,
    decompress,
    find_pattern_in_bytes,
    get_encodings_from_http_headers,
    yield_bytes_from_file,
)

# Typing imports
from typing import Dict, Generator, List, Optional, Union

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
    _bytes: Optional[builtins.bytes] = field(repr=False, default=None)
    _file_handle: Optional[ArchiveFileHandle] = field(repr=False, default=None)

    def __post_init__(self) -> None:
        self.length = self.end - self.start

    @property
    def bytes(self) -> builtins.bytes:
        """
        Load all the bytes into memory and return them as a bytestring.
        """
        if self._bytes is None:
            data = bytearray()
            for chunk in self.iterator():
                data.extend(chunk)
            return bytes(data)
        return self._bytes

    def iterator(self, chunk_size: int = 1024) -> Generator[builtins.bytes, None, None]:
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
            for chunk in yield_bytes_from_file(
                self._file_handle, self.start, self.end, chunk_size
            ):
                yield chunk


@dataclass
class Record(ByteRange):
    """
    A WARC record
    http://iipc.github.io/warc-specifications/specifications/warc-format/warc-1.1/#warc-record

    Comprises a WARC record header and a WARC record content block.
    """

    header: Optional[Header] = None
    content_block: Optional[ContentBlock] = None
    content_length_check_result: Optional[int] = None

    def check_content_length(self) -> None:
        """
        Valid WARC record headers include a Content-Length field that specifies the number of bytes
        in the record's content block.
        http://iipc.github.io/warc-specifications/specifications/warc-format/warc-1.1/#content-length-mandatory

        Search for the content length in the header, and compare it against the number of bytes
        detected when the WARC file was parsed.
        """
        if self.header is None:
            raise SplitRecordsRequiredError("check_content_length", "parsed headers")
        if self.content_block is None:
            raise SplitRecordsRequiredError(
                "check_content_length", "parsed content blocks"
            )

        match = find_pattern_in_bytes(CONTENT_LENGTH_PATTERN, self.header.bytes)

        if match:
            expected = int(match.group(1))
            self.content_length_check_result = self.content_block.length == expected
            logger.debug(
                f"Record content length check: found {self.content_block.length}, expected {expected}."
            )
        else:
            self.content_length_check_result = False

    def get_http_header_block(self) -> Optional[builtins.bytes]:
        """
        If this WARC record describes an HTTP exchange, extract the HTTP headers of that exchange.
        """
        if self.content_block is None:
            raise SplitRecordsRequiredError("get_http_header_block")
        # We expect WARC records that describe HTTP exchanges to have a Content-Type that contains "application/http".
        # http://iipc.github.io/warc-specifications/specifications/warc-format/warc-1.1/#content-type
        if record_content_type_filter("http")(self) and self.content_block.bytes.find(
            CRLF * 2
        ):
            return self.content_block.bytes.split(CRLF * 2)[0]
        return None

    def get_http_body_block(self) -> Optional[builtins.bytes]:
        """
        If this WARC record describes an HTTP exchange, extract the HTTP body of that exchange (if any).
        """
        if self.content_block is None:
            raise SplitRecordsRequiredError("get_http_body_block")
        # We expect WARC records that describe HTTP exchanges to have a Content-Type that contains "application/http".
        # http://iipc.github.io/warc-specifications/specifications/warc-format/warc-1.1/#content-type
        if record_content_type_filter("http")(self) and self.content_block.bytes.find(
            CRLF * 2
        ):
            parts = self.content_block.bytes.split(CRLF * 2, 1)
            if len(parts) == 2:
                return parts[1]
        return None

    def get_decompressed_http_body(self) -> Optional[builtins.bytes]:
        if self.content_block is None:
            raise SplitRecordsRequiredError("get_decompressed_http_body")
        if record_content_type_filter("http")(self) and self.content_block.bytes.find(
            CRLF * 2
        ):
            parts = self.content_block.bytes.split(CRLF * 2, 1)
            if len(parts) == 2 and parts[1]:
                encodings, chunked = get_encodings_from_http_headers(parts[0])
                if encodings:
                    if "zstd" in encodings and chunked:
                        compressed_data = concatenate_chunked_http_response(parts[1])
                    else:
                        compressed_data = parts[1]
                    return decompress(compressed_data, encodings)
                else:
                    return parts[1]
        return None


@dataclass
class Header(ByteRange):
    """
    A WARC record header
    http://iipc.github.io/warc-specifications/specifications/warc-format/warc-1.1/#warc-record-header
    """

    _parsed_fields: Optional[Dict[builtins.bytes, List[Optional[builtins.bytes]]]] = (
        field(repr=False, default=None)
    )

    @classmethod
    def parse_bytes_into_fields(
        cls, data: builtins.bytes
    ) -> Dict[builtins.bytes, List[Optional[builtins.bytes]]]:
        # Line folding is not supported https://github.com/iipc/warc-specifications/issues/74
        headers: defaultdict[builtins.bytes, List[Optional[builtins.bytes]]] = (
            defaultdict(list)
        )
        for line in data.split(CRLF):
            if line:
                split = line.split(b":", 1)
                if len(split) == 1:
                    headers[line].append(None)
                else:
                    headers[split[0]].append(split[1].strip())
        return dict(headers)

    def get_parsed_fields(
        self, decode: bool = False
    ) -> Union[
        Dict[builtins.bytes, List[Optional[builtins.bytes]]],
        Dict[str, List[Optional[str]]],
    ]:
        if self._parsed_fields is None:
            data = self.parse_bytes_into_fields(self.bytes)
        else:
            data = self._parsed_fields
        if decode:
            decoded_data: Dict[str, List[Optional[str]]] = {}
            for field, value_list in data.items():
                decoded_values: List[Optional[str]] = []
                for value in value_list:
                    if value:
                        decoded_values.append(value.decode("utf-8", errors="replace"))
                    else:
                        decoded_values.append(None)
                decoded_data[field.decode("utf-8", errors="replace")] = decoded_values
            return decoded_data
        else:
            return data

    def get_field(
        self,
        field_name: str,
        fallback: Union[str, builtins.bytes, None] = None,
        decode: bool = False,
        return_multiple_values: bool = False,
    ) -> Union[
        str, builtins.bytes, None, List[Optional[str]], List[Optional[builtins.bytes]]
    ]:
        key: Union[str, builtins.bytes]
        if decode:
            key = field_name  # str key for decoded (str) dict
        else:
            key = bytes(field_name, "utf-8")  # bytes key for raw (bytes) dict

        # Type ignore needed because mypy gets upset we are calling get with str and bytes
        # even though that is correct: we always use the right type because of `decode`.
        field = self.get_parsed_fields(decode=decode).get(key)  # type: ignore[arg-type]

        if field is None:
            return fallback
        if return_multiple_values:
            return field
        return field[0]


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


@dataclass
class GzippedMember(ByteRange):
    """
    A "member" of a gzipped file.

    Excerpt from the WARC spec "Annex D: (informative) Compression recommendations"
    http://iipc.github.io/warc-specifications/specifications/warc-format/warc-1.1/#record-at-time-compression

    > Section 12.2 Record-at-time compression
    >
    > As specified in 2.2 of the GZIP specification (see [RFC 1952]), a valid GZIP
    > file consists of any number of GZIP “members”, each independently compressed.
    >
    > Where possible, this property should be exploited to compress each record of
    > a WARC file independently. This results in a valid GZIP file whose per-record
    > subranges also stand alone as valid GZIP files.
    >
    > External indexes of WARC file content may then be used to record each record’s
    > starting position in the GZIP file, allowing for random access of individual
    > records without requiring decompression of all preceding records.
    >
    > Note that the application of this convention causes no change to the uncompressed
    > contents of an individual WARC record.
    """

    _uncompressed_file_handle: Optional[ArchiveFileHandle] = field(
        repr=False, default=None
    )
    _uncompressed_bytes: Optional[builtins.bytes] = field(repr=False, default=None)

    # If the gzip file were decompressed in its entirety, where this member's
    # uncompressed data would begin and end in the resulting file.
    uncompressed_start: Optional[int] = None
    uncompressed_end: Optional[int] = None

    # If this member is decompressed and successfully parsed into a WARC record,
    # the Record object.
    uncompressed_warc_record: Optional[Record] = None

    # If this member is decompressed and does not seem to comprise a WARC record,
    # the raw uncompressed bytes.
    uncompressed_non_warc_data: Optional[builtins.bytes] = None

    def __post_init__(self) -> None:
        super().__post_init__()
        if self.uncompressed_end is not None and self.uncompressed_start is not None:
            self.uncompressed_length = self.uncompressed_end - self.uncompressed_start

    @property
    def uncompressed_bytes(self) -> builtins.bytes:
        """
        Load all the bytes into memory and return them as a bytestring.
        """
        if self._uncompressed_bytes is None:
            data = bytearray()
            for chunk in self.iterator(compressed=False):
                data.extend(chunk)
            return bytes(data)
        return self._uncompressed_bytes

    def iterator(
        self, chunk_size: int = 1024, *, compressed: bool = True
    ) -> Generator[builtins.bytes, None, None]:
        """
        Returns an iterator that yields the bytes in chunks.
        """
        if compressed:
            if self._bytes:
                for i in range(0, len(self._bytes), chunk_size):
                    yield self._bytes[i : i + chunk_size]

            else:
                if not self._file_handle:
                    raise ValueError(
                        "To access member bytes, you must either enable_lazy_loading_of_bytes or "
                        "cache_member_bytes/cache_record_bytes/cache_header_bytes/cache_content_block_bytes."
                    )

                logger.debug(f"Reading from {self.start} to {self.end}.")
                for chunk in yield_bytes_from_file(
                    self._file_handle, self.start, self.end, chunk_size
                ):
                    yield chunk

        else:
            if self._uncompressed_bytes:
                for i in range(0, len(self._uncompressed_bytes), chunk_size):
                    yield self._uncompressed_bytes[i : i + chunk_size]

            else:
                if not self._uncompressed_file_handle:
                    raise ValueError(
                        "To access uncompressed member bytes, you must either cache_member_uncompressed bytes "
                        "or enable_lazy_loading_of_bytes and use decompression style 'file'."
                    )

                logger.debug(
                    f"Reading from {self.uncompressed_start} to {self.uncompressed_end} (decompressed)."
                )
                for chunk in yield_bytes_from_file(
                    self._uncompressed_file_handle,
                    self.uncompressed_start,  # type: ignore[arg-type]
                    self.uncompressed_end,  # type: ignore[arg-type]
                    chunk_size,
                ):
                    yield chunk
