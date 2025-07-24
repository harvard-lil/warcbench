# WARCbench 🛠️

A tool for exploring, analyzing, transforming, recombining, and extracting data from WARC (Web ARChive) files.

<a href="https://tools.perma.cc"><img src="https://github.com/harvard-lil/tools.perma.cc/blob/main/perma-tools.png?raw=1" alt="Perma Tools" width="150"></a>

[![Coverage](https://codecov.io/gh/harvard-lil/warcbench/branch/main/graph/badge.svg)](https://codecov.io/gh/harvard-lil/warcbench)

---

## Contents

- [Quickstart](#quickstart)
- [About](#about)
- [Command line usage](#command-line-usage)
- [Python usage](#python-usage)
- [Configuration](#configuration)
- [Development setup](#development-setup)

---

## Quickstart

To install WARCbench, use Pip:

```sh
# Using HTTPS...
pip install git+https://github.com/harvard-lil/warcbench.git

# ...or SSH:
pip install git+ssh://git@github.com/harvard-lil/warcbench.git
```

Once WARCbench is installed, you may run it on the command line...

```sh
wb summarize example.com.warc
```

...or import it in your Python project:

```python
from warcbench import WARCParser

with open('example.com.warc', 'rb') as warc_file:
    parser = WARCParser(warc_file)
    parser.parse()
```

[⇧ Back to top](#contents)

---

## About

WARCbench has been designed as a resilient, efficient, and highly configurable tool for working with WARC files in all their variety. Among our motivations for the project:

- Enable users to explore a WARC without prior knowledge of the format
- Support inspection of malformed or misbehaving WARCs
- Everything is configurable: plenty of hooks and custom callbacks
- Flexibility to optimize for memory, speed, or convenience as needed
- As little magic as possible: e.g., don't decode bytes into strings or deserialize headers until you need to

Many other useful open-source WARC packages can be found online. Among the inspirations for WARCbench are:

- [Warchaeology](https://github.com/nlnwa/warchaeology)
- [WARCAT](https://github.com/chfoo/warcat)
- [WARCIO](https://github.com/webrecorder/warcio)
- [Warctools](https://github.com/internetarchive/warctools)
- [warc](https://github.com/internetarchive/warc)

WARCbench is a project of the [Harvard Library Innovation Lab](https://lil.law.harvard.edu).

[⇧ Back to top](#contents)

---

## Command line usage

After installing WARCbench, you may use `wb` to interact with WARC files on the command line:

```console
user@host~$ wb inspect example.com.warc

Record bytes 0-280

WARC/1.1
WARC-Filename: archive.warc
WARC-Date: 2024-11-04T19:10:55.900Z
WARC-Type: warcinfo
...
```

All commands support `.warc`, `.warc.gz`, and `.wacz` file formats.

To view a complete summary of WARCbench commands and options, invoke the `--help` flag:

```console
user@host~$ wb --help

Usage: wb [OPTIONS] COMMAND [ARGS]...

  WARCbench command framework

Options:
  -o, --out [raw|json]            Format subcommand output as a human-readable
                                  report (raw) or as JSON.
  -v, --verbose                   Logging verbosity; repeatable.
  -d, --decompression [python|system]
                                  Use native Python or system tools for
                                  extracting archives.  [default: python]
  --gunzip / --no-gunzip          Gunzip the input archive before parsing, if
                                  it is gzipped.  [default: no-gunzip]
  -V, --version                   Show the version and exit.
  -h, --help                      Show this message and exit.

Commands:
  compare-headers     Compare the record headers of two archives.
  compare-parsers     Compare all available parsing strategies.
  extract             Extract files of MIMETYPE to disk.
  filter-records      Filter records; optionally extract to a new archive.
  inspect             Get detailed record metadata.
  match-record-pairs  Match requests/responses into pairs.
  summarize           Summarize the contents of an archive.
...
```

Each subcommand has its own, more-detailed `--help` text. For example, `filter-records`:

```console

user@host~$ wb filter-records --help

Usage: wb filter-records [OPTIONS] FILEPATH

  Applies the specified filters (if any) to the archive's records. If no
  filters are specified, all WARC records are considered to match.

  By default, outputs the number of matching records. Use the `--output-*`
  options to include more detailed information about matching records, or
  `--no-output-count` to suppress the count.

  Can also extract the matching records to a new WARC file (`--extract-to-
  warc`, `--extract-to-gzipped-warc`). To ensure the new WARC includes a
  `WARC-Type: warcinfo` record (if present in the original), even if it would
  otherwise be filtered out by any applied filters, run with `--force-include-
  warcinfo`.

  If extracting records to a new WARC file, by default, no other output is
  produced. To produce a summary report as well, run with `--extract-summary-
  to`.

  To apply your own, custom filters, use `--custom-filter-path` to specify the
  path to a python file where the custom filter functions are listed, in
  desired order of application, in `__all__`. See `tests/assets/custom-
  filters.py` for an example. See the "Filters" section of the README for more
  information on constructing filters.

  This command also supports custom record handlers, which can be used to do
  arbitrary work on records that pass through the supplied filters. For
  example, you could use record handlers to construct a custom report, or
  export records one-at-a-time to an upstream service. Use `--custom-record-
  handler-path` to specify the path to a python file where the custom handler
  functions are listed, in desired order of application, in `__all__`. See
  `tests/assets/custom-handlers.py` for an example. See the "Handlers" section
  of the README for more information on constructing handlers.

  ---

  Example:

      $ wb filter-records --filter-by-warc-named-field Type response tests/assets/example.com.warc
      Found 6 records.

Options:
  --filter-by-http-header TEXT...
                                  Find records with WARC-Type: {request,
                                  response} and look for the supplied HTTP
                                  header name and value.
  --filter-by-http-response-content-type TEXT
                                  Find records with WARC-Type: response, and
                                  then filters by Content-Type.
  --filter-by-http-status-code INTEGER
                                  Find records with WARC-Type: response, and
                                  then filters by HTTP status code.
  --filter-by-http-verb TEXT      Find records with WARC-Type: request, and
                                  then filter by HTTP verb.
...
```

[⇧ Back to top](#contents)

---

## Python usage

### Parsing a WARC file

The `WARCParser` class is typically the best way to start interacting with a WARC file in Python:

```python
from warcbench import WARCParser

# Instantiate a parser, passing in a file handle along with any other config
with open('example.com.warc', 'rb') as warc_file:
    parser = WARCParser(warc_file)

    # Iterate lazily over each record in the WARC...
    for record in parser.iterator():
        print(record.bytes)

    # ...or parse the entire file and produce a list of all records
    parser.parse(cache_records=True)
    print(len(parser.records))
    print(parser.records[3].header.bytes)
```

### Parsing a Gzipped WARC file

You can parse and interact with a gzipped WARC file without gunzipping it using the `WARCGZParser` class.

This is not only for convenience, but for utility: by convention, WARCs are frequently gzipped [one record at a time](http://iipc.github.io/warc-specifications/specifications/warc-format/warc-1.1/#record-at-time-compression), such that a complete `warc.gz` file is in fact a series of concatenated, individually valid gzip files, or "members". This makes it possible to extract individual WARC records, if the byte offsets of their members are known in advance, without needing to gunzip the entire file, which in certain applications can be a significant performance improvement.

```python
from warcbench import WARCGZParser

# Instantiate a parser, passing in a file handle along with any other config
with open('example.com.warc.gz', 'rb') as warcgz_file:
    parser = `WARCGZParser(warcgz_file)

    # Iterate lazily over each record in the WARC...
    for record in parser.iterator(yield_type="records"):
        print(record.start, record.length)

    # ... or over each gzipped member...
    for member in parser.iterator(yield_type="members"):
        print(member.start, member.length, member.record.bytes)

    # ...or parse the entire file and produce a list of all members and records
    parser.parse(cache_members=True)
    print(len(parser.members))
    print(len(parser.records))
    print(parser.records[3].header.bytes)
```

### Utility functions

For other use cases, such as extracting and working with WARCs in a WACZ file, you may wish to use WARCbench's utility functions:

```python
from warcbench import WARCParser
from warcbench.utils import python_open_archive, system_open_archive

# Slower: uses Python zip/gzip to decompress
with python_open_archive('example.com.wacz') as warcgz_file:
    parser = WARCGZParser(warcgz_file)

with python_open_archive('example.com.wacz', gunzip=True) as warc_file:
    parser = WARCParser(warc_file)

# Faster: uses system zip/gzip to decompress where possible
with system_open_archive('example.com.wacz') as warcgz_file:
    parser = WARCGZParser(warcgz_file)

with system_open_archive('example.com.wacz', gunzip=True) as warc_file:
    parser = WARCParser(warc_file)
```

### Filters, handlers, and callbacks

WARCbench includes several additional mechanisms for wrangling WARC records: filters, handlers, and callbacks.

#### Filters

**Record Filters** are functions that include or exclude a WARC record based on a given condition. You can pass in any function that accepts a `warcbench.models.Record` as its sole argument and returns a Boolean value. (A number of built-in filters are included in the `warcbench.filters` module.) Example:

```python
from warcbench import WARCGZParser
from warcbench.config import WARCGZProcessorConfig
from warcbench.filters import warc_named_field_filter
from warcbench.utils import system_open_archive

with system_open_archive('example.com.wacz') as warcgz_file:
    parser = WARCGZParser(
        warcgz_file,
        processors=WARCGZProcessorConfig(
            record_filters=[
                warc_named_field_filter('type', 'request'),
            ]
        )
    )
```

**Member Filters** (only supported when using the WARCGZParser) behave just like record filters, except they work with `warcbench.models.GzippedMember` objects instead of `Record`s. Example:

```python
from warcbench import WARCGZParser
from warcbench.config import WARCGZProcessorConfig
from warcbench.utils import system_open_archive

with system_open_archive('example.com.wacz') as warcgz_file:
    parser = WARCGZParser(
        warcgz_file,
        processors=WARCGZProcessorConfig(
            member_filters=[
                # only yield malformed members
                lambda member: bool(member.uncompressed_non_warc_data),
            ]
        )
    )
```

#### Handlers

**Record handlers** are functions that process a record once it is parsed. For example, you could use a record handler to print each record's content in bytes for debugging purposes, or write each record to disk as a separate file. As with filters, you may pass in an arbitrary handler function that accepts a `warcbench.models.Record` as its sole argument; a handler's return value is ignored. Example:

```python
from warcbench import WARCParser
from warcbench.config import WARCProcessorConfig
from warcbench.record_handlers import get_record_offsets
from warcbench.utils import system_open_archive

with system_open_archive('example.com.warc') as warc_file:
    parser = WARCParser(
        warc_file,
        processors=WARCProcessorConfig(
            record_handlers=[
                get_record_offsets(),
            ]
        )
    )
```

To support inspection of WARC files that contain invalid records, WARCbench also includes a way to specify handlers for unparsable lines. **Unparsable line handlers** behave just like record handlers, except that they accept `warcbench.models.UnparsableLine` objects instead of `Record`s. You could use these handlers to print information about unparsable lines, or even repair them. Example:

```python
from warcbench import WARCParser
from warcbench.config import WARCProcessorConfig
from warcbench.record_handlers import get_record_offsets
from warcbench.utils import system_open_archive

with system_open_archive('example.com.wacz') as warc_file:
    parser = WARCParser(
        warc_file,
        processors=WARCProcessorConfig(
            unparsable_line_handlers=[
                lambda line: print(line),
            ]
        )
    )
```

#### Callbacks

**Callbacks** are functions that run after the WARCbench parser finishes parsing a WARC file. A callback can be any function that accepts a `warcbench.WARCParser` or `warcbench.WARCGZParser` object as its sole argument. You could use a callback to print the number of records parsed, write the records out to disk, pass the full set of records over to another function, and so on.

#### Combining filters, handlers, and callbacks

Filters, handlers, and callbacks are additive, but you can combine them together to produce output of arbitrary complexity. Example:

```python
from warcbench import WARCGZParser
from warcbench.config import WARCGZProcessorConfig
from warcbench.filters import warc_named_field_filter
from warcbench.utils import system_open_archive

def combo_filter(record):
    is_warc_info = lambda r: warc_named_field_filter('type', 'warcinfo')(r)

    targets_example_page = lambda r: warc_named_field_filter(
        'target-uri',
        'http://example.com/',
        exact_match=True
    )(r)

    return is_warc_info(record) or (
        targets_example_page(record) and
        http_verb_filter('get')(record) and
        http_status_filter(200)(record)
    )

with system_open_archive('example.com.wacz') as warcgz_file:
    parser = WARCGZParser(
        warcgz_file,
        processors=WARCGZProcessorConfig(
            record_filters=[
                combo_filter,
                record_content_length_filter('2056', 'le'),
            ]
        )
    )
```

### Configuration

WARCbench supports a number of configuration options:

- You can parse a WARC file by reading the WARC record headers' `Content-Length` fields (faster), or by scanning and splitting on the delimiter expected between WARC records (slower; may rarely detect false positives; more robust against mangled or broken WARCs).

- You can parse a gzipped WARC file by reading and parsing the file member by member (much slower; simpler), or by gunzipping the entire file while making note of member/record boundaries, and then further processing the bytes of the decompressed records (much faster; may use more disk space).

- You can choose whether or not to attempt to split WARC records into headers and content blocks.

- You can choose whether to cache record properties, such as the bytes of headers or content blocks, on the parser object as it proceeds, or to instead consume those bytes lazily on access.

See `config.py` for details.

[⇧ Back to top](#contents)

---

## Development setup

We use [uv](https://docs.astral.sh/uv/) for package dependency management, [Ruff](https://docs.astral.sh/ruff/) for code linting/formatting, and [pytest](https://docs.pytest.org/en/stable/) for testing.

To set up a local development environment, follow these steps:

- [Install uv](https://docs.astral.sh/uv/getting-started/installation/) if it is not already installed
- Clone this repository
- From the project root, `uv sync` to set up a virtual environment and install dependencies

### Linting/formatting

Run the linting process like so:

```sh
uv run ruff check
```

Run the formatting process like so:

```sh
# Check formatting changes before applying
uv run ruff format --check

# Apply formatting changes
uv run ruff format
```

### Tests

Run tests like so:

```sh
uv run pytest
```

### Coverage

Run tests with coverage reporting:

```sh
# Terminal coverage report
uv run pytest --cov=src/warcbench

# HTML coverage report (opens in browser)
uv run pytest --cov=src/warcbench --cov-report=html
```

The HTML report will be generated in the `htmlcov/` directory. Open `htmlcov/index.html` in your browser to view the detailed coverage report.

[⇧ Back to top](#contents)
