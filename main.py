from dataclasses import dataclass
import functools
import gzip
import os
import re
from typing import Optional
import zipfile


#
# Models
#

@dataclass
class ByteRange():
    start: int
    end: int
    bytes: bytes

    def __post_init__(self):
        self.length = self.end - self.start


@dataclass
class UnparsableLine(ByteRange):
    pass


@dataclass
class Record(ByteRange):

    content_length_check_result: Optional[int] = None

    def __post_init__(self):
        super().__post_init__()
        self.header, self.content_block = split_record(self)

    def check_content_length(self):
        match = re.search(rb'Content-Length:\s*(\d+)', self.header.bytes, re.IGNORECASE)

        if match:
            expected = int(match.group(1))
            self.content_length_check_result = self.content_block.length == expected
            print(f"Found {self.content_block.length}, expected {expected}.")
        else:
            self.content_length_check_result = False


@dataclass
class Header(ByteRange):
    pass


@dataclass
class ContentBlock(ByteRange):
    pass


#
# Utils
#

def skip_leading_whitespace(file_handle):
    while True:
        byte = file_handle.read(1)
        if not byte.isspace():
            # Skip the cursor back one byte, so this non-whitespace
            # byte is isn't skipped
            file_handle.seek(-1, whence=os.SEEK_CUR)
            break


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
    last_line_was_a_break = False

    while True:
        line = file_handle.readline()

        if not line:
            end_position = None
            break

        if line == b"\r\n":
            if last_line_was_a_break:
                end_position = file_handle.tell()
                break
            else:
                last_line_was_a_break = True
        else:
            last_line_was_a_break = False

    return end_position


def split_record(record):
    header_start = record.start
    header_end_index = record.bytes.find(b"\r\n\r\n")
    header_end = header_start + header_end_index

    content_block_start_index = header_end_index + len(b"\r\n\r\n")
    content_block_start = record.start + content_block_start_index
    content_block_end = record.end

    header = Header(
        start=header_start,
        end=header_end,
        bytes=record.bytes[:header_end_index]
    )

    content_block = ContentBlock(
        start=content_block_start,
        end=content_block_end,
        bytes=record.bytes[content_block_start_index:]
    )

    return (header, content_block)



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
    def __init__(self, file_handle, check_content_lengths=False):
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
        self.records = []
        self.unparsable_lines = []
        self.warnings = []
        self.error = None

    def parse(self):
        self.file_handle.seek(0)

        while self.state != STATES['END']:
            transition_func = self.transitions[self.state]
            self.state = transition_func()

    def find_warc_header(self):
        skip_leading_whitespace(self.file_handle)
        initial_position = self.file_handle.tell()
        first_line = self.file_handle.readline()
        self.file_handle.seek(initial_position)

        if first_line == b'WARC/1.1\r\n':
            return STATES['EXTRACT_HEADER']
        else:
            self.error = 'No WARC header found.'
            return STATES['END']

    def find_next_record(self):
        skip_leading_whitespace(self.file_handle)
        while True:
            initial_position = self.file_handle.tell()
            if self.file_handle.peek(4).startswith(b"WARC"):
                return STATES['EXTRACT_NEXT_RECORD']

            next_line = self.file_handle.readline()
            current_position = self.file_handle.tell()
            if next_line:
                self.unparsable_lines.append(
                    UnparsableLine(
                        start=initial_position,
                        end=current_position,
                        bytes=next_line
                    )
                )
            else:
                return STATES['END']

    def extract_next_record(self):
        start = self.file_handle.tell()
        stop = find_record_end(self.file_handle)
        if stop:
            end = stop - len(b"\r\n\r\n")
            data = self.file_handle.read(end - start)
            self.file_handle.read(len(b"\r\n\r\n"))
        else:
            self.warnings.append('Last record may have been truncated.')
            data = self.file_handle.read()
            end = self.file_handle.tell()

        record = Record(
            start=start,
            end=end,
            bytes=data
        )
        if self.check_content_lengths:
            record.check_content_length()
        self.records.append(record)

        if end:
            return STATES['FIND_NEXT_RECORD']
        return STATES['END']

    def extract_warc_header(self):
        self.extract_next_record()
        return STATES['FIND_NEXT_RECORD']


with open("579F-LLZR.wacz", "rb") as wacz_file, \
    zipfile.Path(wacz_file, "archive/data.warc.gz").open("rb") as warc_gz_file, \
    gzip.open(warc_gz_file, "rb") as warc_file:
        parser = WARCParser(warc_file, check_content_lengths=True)
        parser.parse()
        breakpoint()

