from warcbench.parsers import (
    DelimiterWARCParser,
    ContentLengthWARCParser,
    GzippedWARCMemberParser,
    GzippedWARCDecompressingParser,
)
from warcbench.config import (
    WARCParsingConfig,
    WARCGZParsingConfig,
    WARCProcessorConfig,
    WARCGZProcessorConfig,
    WARCCachingConfig,
    WARCGZCachingConfig,
)
from typing import Optional


class WARCParser:
    def __init__(
        self,
        file_handle,
        enable_lazy_loading_of_bytes=True,
        parsing_options: Optional[WARCParsingConfig] = None,
        processors: Optional[WARCProcessorConfig] = None,
        cache: Optional[WARCCachingConfig] = None,
    ):
        # Set up default config
        if parsing_options is None:
            parsing_options = WARCParsingConfig()
        if cache is None:
            cache = WARCCachingConfig()
        if processors is None:
            processors = WARCProcessorConfig()

        # Initialize the appropriate parser
        match parsing_options.style:
            case "delimiter":
                self._parser = DelimiterWARCParser(
                    file_handle=file_handle,
                    enable_lazy_loading_of_bytes=enable_lazy_loading_of_bytes,
                    parsing_options=parsing_options,
                    processors=processors,
                    cache=cache,
                )
            case "content_length":
                self._parser = ContentLengthWARCParser(
                    file_handle=file_handle,
                    enable_lazy_loading_of_bytes=enable_lazy_loading_of_bytes,
                    parsing_options=parsing_options,
                    processors=processors,
                    cache=cache,
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

    def parse(self, cache_records=True):
        return self._parser.parse(cache_records)

    def iterator(self):
        return self._parser.iterator()

    def get_record_offsets(self, split=False):
        return self._parser.get_record_offsets(split)

    def get_approximate_request_response_pairs(self, count_only=False):
        return self._parser.get_approximate_request_response_pairs(count_only)


class WARCGZParser:
    def __init__(
        self,
        file_handle,
        enable_lazy_loading_of_bytes=True,
        parsing_options: Optional[WARCGZParsingConfig] = None,
        processors: Optional[WARCGZProcessorConfig] = None,
        cache: Optional[WARCGZCachingConfig] = None,
    ):
        # Set up default config
        if parsing_options is None:
            parsing_options = WARCGZParsingConfig()
        if cache is None:
            cache = WARCGZCachingConfig()
        if processors is None:
            processors = WARCGZProcessorConfig()

        # Validate config
        if (
            enable_lazy_loading_of_bytes
            and parsing_options.decompression_style != "file"
        ):
            raise ValueError(
                "The lazy loading of bytes is only supported when decompression style is 'file'."
            )

        # Initialize the appropriate parser
        match parsing_options.style:
            case "split_gzip_members":
                if parsing_options.decompression_style == "member":
                    self._parser = GzippedWARCMemberParser(
                        file_handle=file_handle,
                        enable_lazy_loading_of_bytes=False,
                        parsing_options=parsing_options,
                        processors=processors,
                        cache=cache,
                    )
                elif parsing_options.decompression_style == "file":
                    self._parser = GzippedWARCDecompressingParser(
                        file_handle=file_handle,
                        enable_lazy_loading_of_bytes=enable_lazy_loading_of_bytes,
                        parsing_options=parsing_options,
                        processors=processors,
                        cache=cache,
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

    @property
    def records(self):
        return self._parser.records

    def parse(self, cache_members=True):
        return self._parser.parse(cache_members)

    def iterator(self, yield_type="members"):
        return self._parser.iterator(yield_type)

    def get_member_offsets(self, compressed=True):
        return self._parser.get_member_offsets(compressed)

    def get_record_offsets(self, split=False):
        return self._parser.get_record_offsets(split)

    def get_approximate_request_response_pairs(self, count_only=False):
        return self._parser.get_approximate_request_response_pairs(count_only)
