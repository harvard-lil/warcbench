"""
`utils` module: Every project has one.
"""

import brotli
from contextlib import contextmanager
from collections import defaultdict, deque
from enum import Enum
import json
import logging
import os
from pyzstd import decompress as pyzstd_decompress
import re
import shutil
import subprocess
import tempfile
from typing import (
    Deque,
    DefaultDict,
    List,
    Tuple,
    TYPE_CHECKING,
    Union,
    Dict,
    Any,
    Iterable,
    cast,
    Optional,
    Generator,
    TextIO,
    IO,
)
import zipfile
import zlib
from io import BufferedReader

from warcbench.exceptions import DecodingException
from warcbench.patterns import CRLF, CONTENT_LENGTH_PATTERN, WARC_VERSIONS
from warcbench.patches import patched_gzip

if TYPE_CHECKING:
    from warcbench.models import Record
    from gzip import GzipFile
    from tempfile import _TemporaryFileWrapper
    from warcbench.patches import EnhancedGzipFile

# Type alias for the various file handle types that our archive functions can return
ArchiveFileHandle = Union[BufferedReader, "GzipFile", "_TemporaryFileWrapper[bytes]"]

logger = logging.getLogger(__name__)


class FileType(Enum):
    GZIPPED_WARC = "gzipped_warc"
    WARC = "warc"


def skip_leading_whitespace(file_handle: ArchiveFileHandle) -> None:
    """Advances the cursor to the first non-whitespace byte."""
    while True:
        byte = file_handle.read(1)
        if not byte.isspace():
            # Skip the cursor back one byte, so this non-whitespace
            # byte is isn't skipped
            file_handle.seek(-1, os.SEEK_CUR)
            break
        else:
            logger.debug("Skipping whitespace!\n")


@contextmanager
def preserve_cursor_position(
    file_handle: ArchiveFileHandle,
) -> Generator[None, None, None]:
    """
    Saves the original cursor position, and returns the file handle there
    when the context manager exits.
    """
    original_position = file_handle.tell()
    try:
        yield
    finally:
        file_handle.seek(original_position)


def advance_to_next_line(
    file_handle: ArchiveFileHandle, chunk_size: int = 1024
) -> Optional[Tuple[bool, bool]]:
    """
    Advance the cursor just past the next newline character.
    Reports if the processed line met either of two special conditions:
    - Did it end in \r\n?
    - Was that \r\n the entire contents of the line?
    Returns:
    - a tuple: (ended_with_crlf, was_crlf_only)
    - or, None, if no explicit line-ending was found
    """
    if chunk_size < 2:
        raise ValueError("Please specify a larger chunk size.")

    last_twoish_bytes_read = bytearray()
    while True:
        chunk = file_handle.read(chunk_size)

        if not chunk:
            return None  # End of file, no explicit line-ending found

        # Special handling, if \r and \n happened to be split between chunks
        if chunk.startswith(b"\n"):
            if last_twoish_bytes_read.endswith(b"\r"):
                # We found a CRLF!
                ended_with_crlf = True
                # Check to see if it was on its own line.
                was_crlf_only = last_twoish_bytes_read.endswith(b"\n\r")
            else:
                ended_with_crlf = False
                was_crlf_only = False

            # Set the cursor to the position after the newline
            file_handle.seek(file_handle.tell() - len(chunk) + 1)
            return ended_with_crlf, was_crlf_only

        # Look for a newline in the current chunk
        newline_index = chunk.find(b"\n")
        if newline_index != -1:
            # Check if the line ends with '\r\n'
            ended_with_crlf = newline_index > 0 and chunk[newline_index - 1] == ord(
                b"\r"
            )

            # Check if the line is just '\r\n'
            was_crlf_only = newline_index == 1 and chunk[0] == ord(b"\r")

            # Set the cursor to the position after the newline
            file_handle.seek(file_handle.tell() - len(chunk) + newline_index + 1)
            return ended_with_crlf, was_crlf_only

        # Update the last two bytes
        last_twoish_bytes_read.clear()
        last_twoish_bytes_read.extend(chunk[-2:])


def find_next_delimiter(
    file_handle: ArchiveFileHandle, chunk_size: int = 1024
) -> Optional[int]:
    """
    WARC records are supposed to be separated by two newlines (\r\n\r\n).
    Attempt to locate the next boundary. May rarely find a false positive.
    """
    with preserve_cursor_position(file_handle):
        last_line_had_a_break = False
        last_line_was_a_break = False

        while True:
            line = advance_to_next_line(file_handle, chunk_size)

            if not line:
                return None  # End of file reached without a record delimiter

            line_ended_with_crlf, line_was_crlf_only = line
            if line_ended_with_crlf:
                # We are only at a record end if this line was just a break.
                if line_was_crlf_only:
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
                        for warc_version in WARC_VERSIONS:
                            if file_handle.peek(len(warc_version)).startswith(
                                warc_version
                            ):
                                # In rare cases, this may catch a false positive...
                                # For instance, an unlikely but random series of bytes in
                                # a content block's payload, or... maybe an uncompressed
                                # HTML page with code blocks about WARC contents :-).
                                # In that case... use a different strategy to parse the WARC.
                                return file_handle.tell()  # End of record found

                    last_line_was_a_break = True

                else:
                    last_line_was_a_break = False
                    last_line_had_a_break = True
            else:
                last_line_was_a_break = False
                last_line_had_a_break = False


def find_next_header_end(
    file_handle: ArchiveFileHandle, chunk_size: int = 1024
) -> Optional[int]:
    """
    WARC record headers are supposed to be separated from their content blocks
    by an empty newline (\r\n). Attempt to find the end of the current header
    (and the start of the next content block).
    """
    with preserve_cursor_position(file_handle):
        while True:
            line = advance_to_next_line(file_handle, chunk_size)

            if not line:
                return None  # End of file reached without finding an end

            _, line_was_crlf_only = line
            if line_was_crlf_only:
                # We've found the line break that's supposed to split
                # a record's head from its content block!
                return file_handle.tell()


def find_content_length_in_bytes(bytes: bytes) -> Optional[int]:
    """
    If a content-length header is present in the passed in bytes, return
    the stated content length as an integer, else return None.
    """
    match = find_pattern_in_bytes(CONTENT_LENGTH_PATTERN, bytes)
    if match:
        return int(match.group(1))
    return None


def find_pattern_in_bytes(
    pattern: bytes, data: bytes, case_insensitive: bool = True
) -> Optional[re.Match[bytes]]:
    """
    Search for a regex pattern in the passed in bytes, return the
    re.Match object if found, else return None.
    https://docs.python.org/3/library/re.html#re.Match
    """
    return re.search(pattern, data, re.IGNORECASE if case_insensitive else 0)


def is_target_in_bytes(
    extracted: bytes,
    target: str,
    case_insensitive: bool = True,
    exact_match: bool = False,
) -> bool:
    """
    Matches the target bytes against the passed in bytestring.
    """
    extracted_bytes = extracted.lower() if case_insensitive else extracted
    target_string = target.lower() if case_insensitive else target
    target_bytes = bytes(target_string, "utf-8")

    if exact_match:
        return target_bytes == extracted_bytes
    return target_bytes in extracted_bytes


def yield_bytes_from_file(
    file_handle: ArchiveFileHandle,
    start_offset: int,
    end_offset: int,
    chunk_size: int = 1024,
) -> Generator[bytes, None, None]:
    """
    An iterator that yields bytes from the file handle in chunks.
    """
    original_position = file_handle.tell()

    file_handle.seek(start_offset)

    while file_handle.tell() < end_offset:
        # Calculate the remaining bytes to read
        remaining_bytes = end_offset - file_handle.tell()

        # Determine the actual chunk size to read
        actual_chunk_size = min(chunk_size, remaining_bytes)
        yield file_handle.read(actual_chunk_size)

    file_handle.seek(original_position)


@contextmanager
def get_archive_filepath(
    wacz_file: Union[str, IO[bytes]],
) -> Generator[str, None, None]:
    """This function extracts the path of the archive in a WACZ file, given its filepath or file handle."""
    with zipfile.Path(wacz_file, "datapackage.json").open("r") as datapackage:
        yield archive_resource(datapackage)


def archive_resource(datapackage: TextIO) -> str:
    """This function extracts the path of an archive from datapackage.json, given its filehandle."""
    data = json.load(datapackage)
    warc_paths = [
        cast(str, resource["path"])
        for resource in data["resources"]
        if resource["path"].lower().endswith(".warc.gz")
    ]
    return warc_paths[0]


@contextmanager
def python_open_archive(
    filepath: str, gunzip: bool = False
) -> Generator[Tuple[ArchiveFileHandle, FileType], None, None]:
    """
    Open a web archive file using native Python packages for decompression.

    Supports WARC, WARC.GZ, and WACZ file formats. This function uses Python's
    built-in compression libraries, which may be slower than system tools but
    provides broader compatibility.

    Args:
        filepath: Path to the archive file (.warc, .warc.gz, or .wacz)
        gunzip: If True, decompress gzipped archives to provide direct access
            to WARC content. If False, provide access to compressed content.

    Yields:
        Tuple[file_handle, FileType]: A tuple containing the opened file handle
        and a FileType enum indicating whether it's a WARC or GZIPPED_WARC.

    Raises:
        ValueError: If the file format is not recognized
        NotImplementedError: If filepath is "-" (stdin not yet supported)

    Example:
        ```python
        from warcbench.utils import python_open_archive
        from warcbench import WARCParser

        with python_open_archive('example.wacz', gunzip=True) as (file, file_type):
            parser = WARCParser(file)
            parser.parse()
        ```
    """
    if filepath.lower().endswith(".wacz"):
        with (
            open(filepath, "rb") as wacz_file,
            get_archive_filepath(wacz_file) as archive,
            zipfile.Path(wacz_file, archive).open("rb") as warc_gz_file,
        ):
            if gunzip:
                with patched_gzip.open(warc_gz_file, "rb") as warc_file:
                    yield (warc_file, FileType.WARC)
            else:
                yield (cast(BufferedReader, warc_gz_file), FileType.GZIPPED_WARC)

    elif filepath.lower().endswith(".warc.gz"):
        with open(filepath, "rb") as warc_gz_file:
            if gunzip:
                with patched_gzip.open(warc_gz_file, "rb") as warc_file:
                    yield (warc_file, FileType.WARC)
            else:
                yield (warc_gz_file, FileType.GZIPPED_WARC)

    elif filepath.lower().endswith(".warc"):
        with open(filepath, "rb") as warc_file:
            yield (warc_file, FileType.WARC)

    elif filepath == "-":
        raise NotImplementedError("stdin not yet available")

    else:
        raise ValueError("This doesn't look like a web archive")


@contextmanager
def system_open_archive(
    filepath: str, gunzip: bool = False
) -> Generator[Tuple[ArchiveFileHandle, FileType], None, None]:
    """
    Open a web archive file using system tools for decompression.

    Supports WARC, WARC.GZ, and WACZ file formats. This function uses system
    unzip and gunzip commands for faster decompression than Python's built-in
    libraries, but requires these tools to be installed.

    Args:
        filepath: Path to the archive file (.warc, .warc.gz, or .wacz)
        gunzip: If True, decompress gzipped archives to provide direct access
            to WARC content. If False, provide access to compressed content.

    Yields:
        Tuple[file_handle, FileType]: A tuple containing the opened file handle
        and a FileType enum indicating whether it's a WARC or GZIPPED_WARC.

    Raises:
        RuntimeError: If required system tools (unzip, gunzip) are not installed
        ValueError: If the file format is not recognized
        NotImplementedError: If filepath is "-" (stdin not yet supported)

    Example:
        ```python
        from warcbench.utils import system_open_archive
        from warcbench import WARCGZParser

        with system_open_archive('example.wacz') as (file, file_type):
            parser = WARCGZParser(file)
            parser.parse()
        ```
    """
    if not shutil.which("unzip"):
        raise RuntimeError("Unzip must be installed.")

    if gunzip and not shutil.which("gunzip"):
        raise RuntimeError("Gunzip must be installed.")

    if filepath.lower().endswith(".wacz"):
        with tempfile.TemporaryDirectory() as tmpdirname:
            subprocess.run(["unzip", "-q", "-d", tmpdirname, filepath])
            with open(f"{tmpdirname}/datapackage.json", "r") as datapackage:
                archive = archive_resource(datapackage)
            if gunzip:
                subprocess.run(["gunzip", f"{tmpdirname}/{archive}"])
                with open(f"{tmpdirname}/{archive[:-3]}", "rb") as warc_file:
                    yield (warc_file, FileType.WARC)
            else:
                with open(f"{tmpdirname}/{archive}", "rb") as warc_gz_file:
                    yield (warc_gz_file, FileType.GZIPPED_WARC)

    elif filepath.lower().endswith(".warc.gz"):
        if gunzip:
            with tempfile.TemporaryDirectory() as tmpdirname:
                shutil.copy(filepath, f"{tmpdirname}/data.warc.gz")
                subprocess.run(["gunzip", f"{tmpdirname}/data.warc.gz"])
                with open(f"{tmpdirname}/data.warc", "rb") as warc_file:
                    yield (warc_file, FileType.WARC)
        else:
            with open(filepath, "rb") as warc_gz_file:
                yield (warc_gz_file, FileType.GZIPPED_WARC)

    elif filepath.lower().endswith(".warc"):
        with open(filepath, "rb") as warc_file:
            yield (warc_file, FileType.WARC)

    elif filepath == "-":
        raise NotImplementedError("stdin not yet available")

    else:
        raise ValueError("This doesn't look like a web archive")


def decompress_and_get_gzip_file_member_offsets(
    file: ArchiveFileHandle,
    outputfile: Optional[ArchiveFileHandle] = None,
    chunk_size: int = 1024,
) -> Deque[Tuple[Tuple[int, int], Tuple[int, int]]]:
    with patched_gzip.open(file, "rb") as gzip_file:
        enhanced_file = cast("EnhancedGzipFile", gzip_file)
        return enhanced_file.decompress_and_get_member_offsets(outputfile, chunk_size)


def find_matching_request_response_pairs(
    records: Iterable["Record"], count_only: bool = False
) -> Dict[str, Any]:
    """
    Returns:
        If count_only=True: {"counts": {"pairs": int, "lone_requests": int, "lone_responses": int}}
        If count_only=False: {
            "pairs_by_uri": Dict[str, List[Tuple[Record, Record]]],
            "lone_requests_by_uri": Dict[str, List[Record]],
            "lone_responses_by_uri": Dict[str, List[Record]],
            "counts": {"pairs": int, "lone_requests": int, "lone_responses": int}
        }
    """
    unpaired_requests_by_uri: DefaultDict[bytes, Deque["Record"]] = defaultdict(deque)
    unpaired_responses_by_uri: DefaultDict[bytes, Deque["Record"]] = defaultdict(deque)
    pairs_by_uri: DefaultDict[bytes, List[Tuple["Record", "Record"]]] = defaultdict(
        list
    )
    lone_requests_by_uri: DefaultDict[bytes, List["Record"]] = defaultdict(list)
    lone_responses_by_uri: DefaultDict[bytes, List["Record"]] = defaultdict(list)
    counts = {"pairs": 0, "lone_requests": 0, "lone_responses": 0}

    for record in records:
        warc_type = cast(bytes, record.header.get_field("WARC-Type"))  # type: ignore[union-attr]
        match warc_type.lower():
            case b"request":
                uri = cast(bytes, record.header.get_field("WARC-Target-URI"))  # type: ignore[union-attr]
                if len(unpaired_responses_by_uri[uri]) > 0:
                    response = unpaired_responses_by_uri[uri].popleft()
                    counts["pairs"] += 1
                    if not count_only:
                        pairs_by_uri[uri].append((record, response))
                else:
                    unpaired_requests_by_uri[uri].append(record)

            case b"response":
                uri = cast(bytes, record.header.get_field("WARC-Target-URI"))  # type: ignore[union-attr]
                if len(unpaired_requests_by_uri[uri]) > 0:
                    request = unpaired_requests_by_uri[uri].popleft()
                    counts["pairs"] += 1
                    if not count_only:
                        pairs_by_uri[uri].append((request, record))
                else:
                    unpaired_responses_by_uri[uri].append(record)

    for uri, request_list in unpaired_requests_by_uri.items():
        length = len(request_list)
        if length > 0:
            counts["lone_requests"] += length
            if not count_only:
                lone_requests_by_uri[uri].extend(request_list)

    for uri, response_list in unpaired_responses_by_uri.items():
        length = len(response_list)
        if length > 0:
            counts["lone_responses"] += length
            if not count_only:
                lone_responses_by_uri[uri].extend(response_list)

    if count_only:
        return {"counts": counts}

    return {
        "pairs_by_uri": dict(pairs_by_uri),
        "lone_requests_by_uri": dict(lone_requests_by_uri),
        "lone_responses_by_uri": dict(lone_responses_by_uri),
        "counts": counts,
    }


def get_encodings_from_http_headers(
    header_block: bytes,
) -> Tuple[Optional[List[str]], Optional[re.Match[bytes]]]:
    """Find content encodings and whether a record is chunked."""
    encodings = None
    match = find_pattern_in_bytes(
        rb"Content-Encoding:\s*(.*)((\r\n)|$)",
        header_block,
        case_insensitive=True,
    )
    if match:
        try:
            encodings = match.group(1).decode("utf-8", errors="replace").split(" ")
        except Exception:
            pass

    chunked = find_pattern_in_bytes(
        rb"Transfer-Encoding:\s*chunked((\r\n)|$)",
        header_block,
        case_insensitive=True,
    )
    return encodings, chunked


def concatenate_chunked_http_response(body_block: bytes) -> bytes:
    """Reassemble chunks of a chunked HTTP response."""
    return b"".join(body_block.split(CRLF)[1::2])


def decompress(http_body_block: bytes, encodings: Optional[List[str]]) -> bytes:
    """This function recursively decodes an HTTP body block, given a list of encodings."""
    if not encodings:
        return http_body_block
    else:
        return decompress(_decompress(http_body_block, encodings[-1]), encodings[:-1])


def _decompress(http_body_block: bytes, encoding: str) -> bytes:
    """This function decodes an HTTP body block, given an encoding."""
    # see https://developer.mozilla.org/en-US/docs/Web/HTTP/Reference/Headers/Content-Encoding
    if encoding == "gzip":
        return zlib.decompress(http_body_block, 16 + zlib.MAX_WBITS)
    elif encoding == "deflate":
        return zlib.decompress(http_body_block, -15)
    elif encoding == "br":
        return brotli.decompress(http_body_block)  # type: ignore[no-any-return]
    elif encoding == "zstd":
        return pyzstd_decompress(http_body_block)
    elif encoding == "dcb":
        raise DecodingException("dcb decoding not yet implemented")
    elif encoding == "dcz":
        raise DecodingException("dcz decoding not yet implemented")

    if not encoding:
        return http_body_block
    else:
        raise DecodingException(f"Unable to recognize encoding {encoding}")
