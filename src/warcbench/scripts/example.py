import click
import gzip
import zipfile

from warcbench import WARCParser


@click.command()
def parse_example() -> None:
    """Parses the WARC in the sample WACZ"""
    with (
        open("assets/example.com.wacz", "rb") as wacz_file,
        zipfile.Path(wacz_file, "archive/data.warc.gz").open("rb") as warc_gz_file,
        gzip.open(warc_gz_file, "rb") as warc_file,
    ):
        parser = WARCParser(
            warc_file,
            # parsing_style="content_length",
            stop_after_nth=3,
            # check_content_lengths=True,
            cache_unparsable_lines=True,
            # split_records=False,
            # cache_record_bytes=True,
            # cache_header_bytes=True,
            # cache_content_block_bytes=True,
            # cache_unparsable_line_bytes=True,
            # enable_lazy_loading_of_bytes=False,
            filters=[
                # lambda record: False,
                # record_content_length_filter(1007),
                # record_content_length_filter(38978, 'gt'),
                # record_content_type_filter('http'),
                # warc_named_field_filter('type', 'warcinfo'),
                # warc_named_field_filter('type', 'request'),
                # warc_named_field_filter('target-uri', 'favicon'),
                # warc_named_field_filter(
                #     'target-uri',
                #     'http://example.com/',
                #     exact_match=True
                # ),
                # http_verb_filter('get'),
                # http_status_filter(200),
                # http_header_filter('content-encoding', 'gzip'),
                # http_response_content_type_filter('pdf'),
                # warc_header_regex_filter('Scoop-Exchange-Description: Provenance Summary'),
            ],
            record_handlers=[
                # print_record_attribute('length')
            ],
            parser_callbacks=[
                # lambda parser: print(len(parser.records))
            ],
            # unparsable_line_handlers=[
            #     lambda line: print(len(line.bytes))
            # ]
        )
        parser.parse()
        click.echo(f"Found {len(parser.records)} records!")
