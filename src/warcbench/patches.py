"""
`patches` module: If we get up to any funny business, we do it here.
"""

from collections import deque
import logging

logger = logging.getLogger(__name__)

#
# Patch gzip._GzipReader such that it tracks the starting and ending offsets
# of every gzipped "member" in the file.
#

import gzip  # noqa: E402

_OrigReader = gzip._GzipReader


class MemberOffsetTrackingGzipReader(_OrigReader):
    def __init__(self, *args, **kwargs):
        self.offsets = deque()
        self.current_member_start_offset = 0
        self.current_member_uncompressed_start_offset = 0
        return super().__init__(*args, **kwargs)

    def read(self, size=-1):
        """
        This is the original `read` method from python 3.10.3, unaltered
        except for references to imports and content explicitly labeled
        as "LIL changes".

        Those changes do one thing only: append the offset of the first byte
        and the last byte of the current "member" in the gzip file to self.offsets,
        such that, after reading through the whole file, self.offsets will contain
        a list explaining how the entire file is divided up.
        """
        if size < 0:
            return self.readall()
        # size=0 is special because decompress(max_length=0) is not supported
        if not size:
            return b""

        # For certain input data, a single
        # call to decompress() may not return
        # any data. In this case, retry until we get some data or reach EOF.
        while True:
            if self._decompressor.eof:
                # Ending case: we've come to the end of a member in the file,
                # so finish up this member, and read a new gzip header.
                # Check the CRC and file size, and set the flag so we read
                # a new member
                self._read_eof()
                self._new_member = True

                #
                # Begin LIL changes
                #
                current_position = self._fp.file.tell()
                unused_bytes = len(self._decompressor.unused_data)
                if unused_bytes:
                    # The 8-byte footer of the member is included in the unused data.
                    # The cursor was advanced past the footer by the call to self._read_eof()
                    # So, the offset of the start of the next record is, the current
                    # cursor location, minus the length of the unused bytes... excepting the footer.
                    current_member_end_offset = current_position - (unused_bytes - 8)
                else:
                    current_member_end_offset = current_position

                offsets = (self.current_member_start_offset, current_member_end_offset)
                self.current_member_start_offset = current_member_end_offset

                uncompressed_offsets = (self.current_member_uncompressed_start_offset, self._pos)
                self.current_member_uncompressed_start_offset = self._pos

                self.offsets.append((offsets, uncompressed_offsets))
                logger.debug(f"Gzipped member from {offsets[0]} to {offsets[1]}.")
                #
                # End LIL changes
                #

                self._decompressor = self._decomp_factory(**self._decomp_args)

            if self._new_member:
                # If the _new_member flag is set, we have to
                # jump to the next member, if there is one.
                self._init_read()
                if not self._read_gzip_header():
                    self._size = self._pos
                    return b""

                self._new_member = False

            # Read a chunk of data from the file
            buf = self._fp.read(patched_gzip.io.DEFAULT_BUFFER_SIZE)

            uncompress = self._decompressor.decompress(buf, size)
            if self._decompressor.unconsumed_tail != b"":
                self._fp.prepend(self._decompressor.unconsumed_tail)
            elif self._decompressor.unused_data != b"":
                # Prepend the already read bytes to the fileobj so they can
                # be seen by _read_eof() and _read_gzip_header()
                self._fp.prepend(self._decompressor.unused_data)

            if uncompress != b"":
                break
            if buf == b"":
                raise EOFError(
                    "Compressed file ended before the end-of-stream marker was reached"
                )

        self._add_read_data(uncompress)
        self._pos += len(uncompress)
        return uncompress


gzip._GzipReader = MemberOffsetTrackingGzipReader


class MemberOffsetTrackingGzipFile(gzip.GzipFile):
    def get_member_offsets(self):
        """
        This is a custom LIL method, that calls our patched file-reading
        logic, and then reports the boundaries of the "members" of the file.
        """
        self.seek(0)
        self.read()
        return self._buffer.raw.offsets


gzip.GzipFile = MemberOffsetTrackingGzipFile

patched_gzip = gzip
del gzip
