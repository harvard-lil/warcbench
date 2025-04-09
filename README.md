# WARCbench 🛠️

A tool for exploring, analyzing, transforming, recombining, and extracting data from WARC (Web ARChive) files.

> [!WARNING]
> WARCbench is currently under active development. Breaking changes are expected as the project moves toward an initial release.

<a href="https://tools.perma.cc"><img src="https://github.com/harvard-lil/tools.perma.cc/blob/main/perma-tools.png?raw=1" alt="Perma Tools" width="150"></a>

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
pip install git+https://github.com/rebeccacremona/warcbench.git

# ...or SSH:
pip install git+ssh://git@github.com/rebeccacremona/warcbench.git
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

To view a complete summary of WARCbench commands and options, invoke the `--help` flag:

```console
user@host~$ wb --help

Usage: wb [OPTIONS] COMMAND [ARGS]...
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

### Utility functions

For other use cases, such as extracting WARCs from a gzipped WACZ file, you may wish to use WARCbench's utility functions:

```python
from warcbench import WARCParser
from warcbench.utils import python_open_archive, system_open_archive

# Slower: uses Python zip/gzip to decompress
with python_open_archive('example.com.wacz') as warc_file:
    parser = WARCParser(warc_file)

# Faster: uses system zip/gzip to decompress
with system_open_archive('example.com.wacz') as warc_file:
    parser = WARCParser(warc_file)
```

### Filters, handlers, and callbacks

WARCbench includes several additional mechanisms for wrangling WARC records: filters, handlers, and callbacks.

#### Filters

**Filters** are functions that include or exclude a WARC record based on a given condition. You can pass in any function that accepts a `warcbench.models.Record` as its sole argument and returns a Boolean value. (A number of built-in filters are included in the `warcbench.filters` module.) Example:

```python
from warcbench import WARCParser
from warcbench.filters import warc_named_field_filter
from warcbench.utils import system_open_archive

with system_open_archive('example.com.wacz') as warc_file:
    parser = WARCParser(
        warc_file,
        record_filters=[
            warc_named_field_filter('type', 'request'),
        ]
    )
```

#### Handlers

**Record handlers** are functions that process a record once it is parsed. For example, you could use a record handler to print each record's content in bytes for debugging purposes, or write each record to disk as a separate file. As with filters, you may pass in an arbitrary handler function that accepts a `warcbench.models.Record` as its sole argument; a handler's return value is ignored. Example:

```python
from warcbench import WARCParser
from warcbench.record_handlers import get_record_offsets
from warcbench.utils import system_open_archive

with system_open_archive('example.com.wacz') as warc_file:
    parser = WARCParser(
        warc_file,
        record_handlers=[
            get_record_offsets(),
        ]
    )
```

To support inspection of WARC files that contain invalid records, WARCbench also includes a way to specify handlers for unparsable lines. **Unparsable line handlers** behave just like record handlers, except that they accept `warcbench.models.UnparsableLine` objects instead of `Record`s. You could use these handlers to print information about unparsable lines, or even repair them. Example:

```python
from warcbench.record_handlers import get_record_offsets
from warcbench.utils import system_open_archive

with system_open_archive('example.com.wacz') as warc_file:
    parser = WARCParser(
        warc_file,
        unparsable_line_handlers=[
            lambda line: print(line),
        ]
    )
```

#### Callbacks

**Callbacks** are functions that run after the WARCbench parser finishes parsing a WARC file. A callback can be any function that accepts a `warcbench.WARCParser` object as its sole argument. You could use a callback to print the number of records parsed, write the records out to disk, pass the full set of records over to another function, and so on.

#### Combining filters, handlers, and callbacks

Filters, handlers, and callbacks are additive, but you can combine them together to produce output of arbitrary complexity. Example:

```python
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

with system_open_archive('example.com.wacz') as warc_file:
    parser = WARCParser(
        warc_file,
        record_filters=[
            combo_filter,
            record_content_length_filter('2056', 'le'),
        ]
    )
```

### Configuration

WARCbench supports a number of configuration options:

- You can parse a WARC file by reading the WARC record headers' `Content-Length` fields (faster), or by scanning and splitting on the delimiter expected between WARC records (slower; may rarely detect false positives; more robust against mangled or broken WARCs).

- You can choose whether or not to attempt to split WARC records into headers and content blocks.

- You can choose whether to cache record properties, such as the bytes of headers or content blocks, on the parser object as it proceeds, or to instead consume those bytes lazily on access.

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

[⇧ Back to top](#contents)
