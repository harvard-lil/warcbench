"""
`utils` module: Every project has one.
"""

from contextlib import contextmanager
import os
import re

from warcbench.logging import logging

logger = logging.getLogger(__name__)


def skip_leading_whitespace(file_handle):
    while True:
        byte = file_handle.read(1)
        if not byte.isspace():
            # Skip the cursor back one byte, so this non-whitespace
            # byte is isn't skipped
            file_handle.seek(-1, whence=os.SEEK_CUR)
            break
        else:
            logger.debug("Skipping whitespace!\n")


@contextmanager
def preserve_cursor_position(file_handle):
    # Save the original position of the file handle
    original_position = file_handle.tell()
    try:
        yield
    finally:
        # Reset the file handle to the original position
        file_handle.seek(original_position)


def advance_to_next_line(file_handle, chunk_size=1024):
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


def find_pattern_in_bytes(pattern, data, case_insensitive=True):
    return re.search(pattern, data, re.IGNORECASE if case_insensitive else 0)


def is_target_in_bytes(extracted, target, case_insensitive=True, exact_match=False):
    extracted_bytes = extracted.lower() if case_insensitive else extracted
    target_string = target.lower() if case_insensitive else target
    target_bytes = bytes(target_string, "utf-8")

    if exact_match:
        return target_bytes == extracted_bytes
    return target_bytes in extracted_bytes
