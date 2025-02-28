from warcbench.parsers import DelimiterWARCParser, ContentLengthWARCParser


class WARCParser:
    def __init__(
        self,
        file_handle,
        parsing_style="delimiter",
        parsing_chunk_size=1024,
        check_content_lengths=False,
        cache_unparsable_lines=False,
        cache_record_bytes=False,
        cache_header_bytes=False,
        cache_content_block_bytes=False,
        cache_unparsable_line_bytes=False,
        enable_lazy_loading_of_bytes=True,
        filters=None,
        unparsable_line_handlers=None,
    ):
        #
        # Validate Options
        #

        if check_content_lengths and parsing_style == "content_length":
            raise ValueError(
                "Checking content lengths is only meaningful when parsing in delimter mode."
            )

        #
        # Set up
        #

        match parsing_style:
            case "delimiter":
                self._parser = DelimiterWARCParser(
                    file_handle=file_handle,
                    parsing_chunk_size=parsing_chunk_size,
                    check_content_lengths=check_content_lengths,
                    cache_unparsable_lines=cache_unparsable_lines,
                    cache_record_bytes=cache_record_bytes,
                    cache_header_bytes=cache_header_bytes,
                    cache_content_block_bytes=cache_content_block_bytes,
                    cache_unparsable_line_bytes=cache_unparsable_line_bytes,
                    enable_lazy_loading_of_bytes=enable_lazy_loading_of_bytes,
                    filters=filters,
                    unparsable_line_handlers=unparsable_line_handlers,
                )
            case "content_length":
                self._parser = ContentLengthWARCParser(
                    file_handle=file_handle,
                    parsing_chunk_size=parsing_chunk_size,
                    cache_unparsable_lines=cache_unparsable_lines,
                    cache_record_bytes=cache_record_bytes,
                    cache_header_bytes=cache_header_bytes,
                    cache_content_block_bytes=cache_content_block_bytes,
                    cache_unparsable_line_bytes=cache_unparsable_line_bytes,
                    enable_lazy_loading_of_bytes=enable_lazy_loading_of_bytes,
                    filters=filters,
                    unparsable_line_handlers=unparsable_line_handlers,
                )
            case _:
                supported_parsing_styles = ["delimiter", "content_length"]
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

    def parse(self, find_first_record_only=False):
        return self._parser.parse(find_first_record_only)

    def iterator(self, find_first_record_only=False):
        return self._parser.iterator(find_first_record_only)
