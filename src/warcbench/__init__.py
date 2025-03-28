from warcbench.parsers import (
    DelimiterWARCParser,
    ContentLengthWARCParser,
    GzippedWARCMemberParser,
    GzippedWARCDecompressingParser,
)


class WARCParser:
    def __init__(
        self,
        file_handle,
        parsing_style="content_length",
        parsing_chunk_size=1024,
        stop_after_nth=None,
        check_content_lengths=False,
        split_records=True,
        cache_unparsable_lines=False,
        cache_record_bytes=False,
        cache_header_bytes=False,
        cache_content_block_bytes=False,
        cache_unparsable_line_bytes=False,
        enable_lazy_loading_of_bytes=True,
        filters=None,
        record_handlers=None,
        unparsable_line_handlers=None,
        parser_callbacks=None,
    ):
        #
        # Validate Options
        #

        if check_content_lengths and parsing_style == "content_length":
            raise ValueError(
                "Checking content lengths is only meaningful when parsing in delimiter mode."
            )

        #
        # Set up
        #

        match parsing_style:
            case "delimiter":
                self._parser = DelimiterWARCParser(
                    file_handle=file_handle,
                    parsing_chunk_size=parsing_chunk_size,
                    stop_after_nth=stop_after_nth,
                    check_content_lengths=check_content_lengths,
                    split_records=split_records,
                    cache_unparsable_lines=cache_unparsable_lines,
                    cache_record_bytes=cache_record_bytes,
                    cache_header_bytes=cache_header_bytes,
                    cache_content_block_bytes=cache_content_block_bytes,
                    cache_unparsable_line_bytes=cache_unparsable_line_bytes,
                    enable_lazy_loading_of_bytes=enable_lazy_loading_of_bytes,
                    filters=filters,
                    record_handlers=record_handlers,
                    unparsable_line_handlers=unparsable_line_handlers,
                    parser_callbacks=parser_callbacks,
                )
            case "content_length":
                self._parser = ContentLengthWARCParser(
                    file_handle=file_handle,
                    parsing_chunk_size=parsing_chunk_size,
                    stop_after_nth=stop_after_nth,
                    split_records=split_records,
                    cache_unparsable_lines=cache_unparsable_lines,
                    cache_record_bytes=cache_record_bytes,
                    cache_header_bytes=cache_header_bytes,
                    cache_content_block_bytes=cache_content_block_bytes,
                    cache_unparsable_line_bytes=cache_unparsable_line_bytes,
                    enable_lazy_loading_of_bytes=enable_lazy_loading_of_bytes,
                    filters=filters,
                    record_handlers=record_handlers,
                    unparsable_line_handlers=unparsable_line_handlers,
                    parser_callbacks=parser_callbacks,
                )
            case _:
                supported_parsing_styles = [
                    "delimiter",
                    "content_length",
                ]
                raise ValueError(
                    f"Supported parsing styles: {', '.join(supported_parsing_styles)}"
                )

    @property
    def warnings(self):
        return self._parser.warnings

    @property
    def error(self):
        return self._parser.error

    @property
    def current_record(self):
        return self._parser.current_record

    @property
    def records(self):
        return self._parser.records

    @property
    def unparsable_lines(self):
        return self._parser.unparsable_lines

    def parse(self):
        return self._parser.parse()

    def iterator(self):
        return self._parser.iterator()


class WARCGZParser:
    def __init__(
        self,
        file_handle,
        parsing_style="split_gzip_members",
        stop_after_nth=None,
        decompress_and_parse_members=True,
        decompression_style="file",
        decompress_chunk_size=1024,
        split_records=True,
        cache_member_bytes=False,
        cache_member_uncompressed_bytes=False,
        cache_record_bytes=False,
        cache_header_bytes=False,
        cache_content_block_bytes=False,
        cache_non_warc_member_bytes=False,
        enable_lazy_loading_of_bytes=True,
        filters=None,
        member_handlers=None,
        parser_callbacks=None,
    ):
        #
        # Validate Options
        #

        if decompress_and_parse_members and parsing_style != "split_gzip_members":
            raise ValueError(
                "Decompressing records only applies to gzip member splitting mode."
            )

        if enable_lazy_loading_of_bytes and decompression_style != "file":
            raise ValueError(
                "The lazy loading of bytes is only supported when decompression style is 'file'."
            )

        #
        # Set up
        #

        match parsing_style:
            case "split_gzip_members":
                if decompression_style == "member":
                    self._parser = GzippedWARCMemberParser(
                        file_handle=file_handle,
                        stop_after_nth=stop_after_nth,
                        decompress_and_parse_members=decompress_and_parse_members,
                        decompress_chunk_size=decompress_chunk_size,
                        split_records=split_records,
                        cache_member_bytes=cache_member_bytes,
                        cache_member_uncompressed_bytes=cache_member_uncompressed_bytes,
                        cache_record_bytes=cache_record_bytes,
                        cache_header_bytes=cache_header_bytes,
                        cache_content_block_bytes=cache_content_block_bytes,
                        cache_non_warc_member_bytes=cache_non_warc_member_bytes,
                        filters=filters,
                        member_handlers=member_handlers,
                        parser_callbacks=parser_callbacks,
                    )
                elif decompression_style == "file":
                    self._parser = GzippedWARCDecompressingParser(
                        file_handle=file_handle,
                        stop_after_nth=stop_after_nth,
                        decompress_and_parse_members=decompress_and_parse_members,
                        decompress_chunk_size=decompress_chunk_size,
                        split_records=split_records,
                        cache_member_bytes=cache_member_bytes,
                        cache_member_uncompressed_bytes=cache_member_uncompressed_bytes,
                        cache_record_bytes=cache_record_bytes,
                        cache_header_bytes=cache_header_bytes,
                        cache_content_block_bytes=cache_content_block_bytes,
                        cache_non_warc_member_bytes=cache_non_warc_member_bytes,
                        enable_lazy_loading_of_bytes=enable_lazy_loading_of_bytes,
                        filters=filters,
                        member_handlers=member_handlers,
                        parser_callbacks=parser_callbacks,
                    )
                else:
                    supported_decompression_styles = [
                        "member",
                        "file",
                    ]
                    raise ValueError(
                        f"Supported decompression styles: {', '.join(supported_decompression_styles)}"
                    )
            case _:
                supported_parsing_styles = [
                    "split_gzip_members",
                ]
                raise ValueError(
                    f"Supported parsing styles: {', '.join(supported_parsing_styles)}"
                )

    @property
    def warnings(self):
        return self._parser.warnings

    @property
    def error(self):
        return self._parser.error

    @property
    def current_member(self):
        return self._parser.current_member

    @property
    def members(self):
        return self._parser.members

    def parse(self):
        return self._parser.parse()

    def iterator(self):
        return self._parser.iterator()
