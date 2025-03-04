"""
`utils` module: Every project has one.
"""

from contextlib import contextmanager
import logging
import os
import re

from warcbench.patterns import CRLF, CONTENT_LENGTH_PATTERN, WARC_VERSION

logger = logging.getLogger(__name__)


def skip_leading_whitespace(file_handle):
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


def find_next_delimiter(file_handle, chunk_size=1024):
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


def find_next_header_end(file_handle, chunk_size=1024):
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


def find_content_length_in_bytes(bytes):
    match = find_pattern_in_bytes(CONTENT_LENGTH_PATTERN, bytes)
    if match:
        return int(match.group(1))
    return None


def find_pattern_in_bytes(pattern, data, case_insensitive=True):
    return re.search(pattern, data, re.IGNORECASE if case_insensitive else 0)


def is_target_in_bytes(extracted, target, case_insensitive=True, exact_match=False):
    extracted_bytes = extracted.lower() if case_insensitive else extracted
    target_string = target.lower() if case_insensitive else target
    target_bytes = bytes(target_string, "utf-8")

    if exact_match:
        return target_bytes == extracted_bytes
    return target_bytes in extracted_bytes
