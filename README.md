# WARCbench 🛠️

Utilities for exploring, analyzing, munging, recombining, and extracting data from WARC (Web ARChive) files.

**Use it in the terminal...**
```bash
wb parse example.com.warc.gz
```

**... or in your python project**
```python
from warcbench import WARCParser

with open("example.com.warc", "rb") as warc_file:
    parser = WARCParser(warc_file)
    parser.parse()
```

<a href="https://tools.perma.cc"><img src="https://github.com/harvard-lil/tools.perma.cc/blob/main/perma-tools.png?raw=1" alt="Perma Tools" width="150"></a>

---

## Summary
-- Preamble [#preamble]
-- About [#about]
-- Installation [#installation]
-- Major Features [#major-features]
-- Using WARCbench on the command line [#using-warcbench-on-the-command-line]
-- Using WARCbench as a python library [#using-warcbench-as-a-python-library]
-- Development [#development]


## Preamble

> ⚠️ Not ready for public release


## About

### Motivation

- make it easy to explore the contents of a WARC without prior experience with the format
- work with malformed or misbehaving WARCs without everything breaking
- give developers total control: hooks and custom callbacks, everything is configurable
- pick your poison: optimize for RAM usage, or speed, or convenience, or etc., depending on your circumstances
- do as little as possible: e.g., don't decode bytes into strings or parse individual WARC headers into a dict until we have some reason to do so

### Alternatives/Inspired By

- https://github.com/nlnwa/warchaeology
- https://github.com/chfoo/warcat
- https://github.com/webrecorder/warcio
- http://code.hanzoarchives.com/warc-tools
- https://github.com/internetarchive/warc

[👆 Back to the summary](#summary)

---

## Installation

While this is still unpublished, there are a few strategies.
(These instructions may not be perfect.)

### With `uv` (recommended)

1) Clone the repo.
2) Run `uv install`

If you are running WARCbench from the command line, make sure you preface your commands with `uv run`.

### With other package managers

You can install without cloning by using your package manager's
syntax for installing straight from the repo. For example:

#### With vanilla pip and virtualenvs:

```bash
python3 -m venv wb-env
. wb-env/bin/activate
pip install git+ssh://git@github.com/rebeccacremona/warcbench.git

# To force re-install a fresh copy:
pip install --force-reinstall git+ssh://git@github.com/rebeccacremona/warcbench.git
```

If you are running WARCbench from the command line, make sure you have activated your virtualenv first.

#### In a requirements.txt file:

```
# Example other regular packages used by your project
pytest>=6.2.4
requests

# This Git repository
git+ssh://git@github.com/rebeccacremona/warcbench.git
```

Then, use `pip install -r requirements.txt` and `pip install -r requirements.txt --force-reinstall` as needed.

If you are running WARCbench from the command line, make sure you have activated your virtualenv first.


### Using `poetry`'s `pyproject.toml`

```
[tool.poetry]
name = "your_project_name"
version = "0.1.0"
description = "A brief description of your project."
authors = ["Your Name <you@example.com>"]

[tool.poetry.dependencies]
# Example other regular packages used by your project
python = "^3.8"
pytest = "^6.2.4"
requests = "^2.25.1"

# Git dependency
warcbench = { git = "ssh://git@github.com/rebeccacremona/warcbench.git" }

# If you want to specify a branch, tag, or commit:
# warcbench = { git = "ssh://git@github.com/rebeccacremona/warcbench.git", branch = "main" }
# warcbench = { git = "ssh://git@github.com/rebeccacremona/warcbench.git", tag = "v1.0.0" }
# warcbench = { git = "ssh://git@github.com/rebeccacremona/warcbench.git", rev = "commit_hash" }

[build-system]
requires = ["poetry-core>=1.0.0"]
build-backend = "poetry.core.masonry.api"
```

Then, use `poetry install` and `poetry install --no-cache` or `poetry update warcbench` as needed.

If you are running WARCbench from the command line, make sure you preface your commands with `poetry run`, or run unprefaced by entering a shell with the virtualenv activated using `poetry shell`.

[👆 Back to the summary](#summary)

---

## Major Features

[👆 Back to the summary](#summary)

---

## Using WARCbench on the command line

[👆 Back to the summary](#summary)

---

## Using WARCbench as a python library

Examples:

```python
from warcbench import WARCParser

# Instantiate a parser, passing in an open file handle, along with any other configuration.
with open('example.com.warc', 'rb') as warc_file:
    parser = WARCParser(warc_file)

    # iterate through each record in the WARC
    for record in parser.iterator():
        print(record.bytes)

    # or, parse the whole file, and get access to a list of all the records at once
    parser.parse()
    print(len(parser.records))
    print(parser.records[3].header.bytes)


# Optionally, use our utilities to open your file: it will handle getting a WARC out of a WACZ and gunzipping it for you, either natively in python (slow performance) or using system executables.

from warcbench.utils import python_open_archive, system_open_archive

with system_open_archive('example.com.wacz') as warc_file:
    parser = WARCParser(warc_file)

# Hook up any filters, handlers, or callbacks you want:
#
# - Filters are functions that cause a record to be skipped. See warcbench.filters
#   to see a list of built in filters, but, you can also pass in any function that
#   takes a warcbench.models.Record as an argument and returns True or False.
#
# - Record handlers do worth on a warcbench.models.Record after it is parsed, after
#   filters, before the next record is parsed. For example, you could use a record handler
#   to print(record.bytes) as the parser goes along, for debugging purposes. Or,
#   for writing a record to disk. Handlers are any function that takes a
#   warcbench.models.Record as an argument. Its return value is ignored. We only
#   have a sampler handler for now, in warcbench.record_handlers, but are planning
#   to add more!
#
# -  Unparsable line handlers do the same thing as record handlers, except operate
#    on warcbench.models.UnparsableLine objects: anything the parser trips over,
#    while parsing the file. You could use these handlers to log information about
#    unparsable lines, or even, together with more custom code, repair them. We
#    don't have any examples yet.
#
# - Parser callbacks run after the parser is finished parsing the file. Callbacks
#   are any function that takes a warcbench.WARCParser object. You could use a
#   callback to print the number of records parsed, pipe the full set of records
#   to other code, write the full set of records to disk, etc. We don't have any
#   examples yet.

from warcbench.filters import (warc_named_field_filter, http_verb_filter, http_status_filter,
    http_response_content_type_filter)

with system_open_archive('example.com.wacz') as warc_file:
    parser = WARCParser(
        warc_file,
        filters = [
            warc_named_field_filter('type', 'request')
        ]
    )

#
# Filters, handlers, and callbacks are additive, but you can combine them together
# to into functions of arbitrary complexity.
#

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
        filters = [
            combo_filter,
            record_content_length_filter('2056', 'le'),
        ]
    )

# Depending on what you want to do with the output, there are lots
# of configuration options.
#
# - You can parse the WARC by reading the WARC record headers' "content-length"
#   fields (faster), or by scanning and splitting on the delimiter expected
#   between WARC records (slower; may rarely detect false positives; more
#   robust against mangled or broken WARCs)
#
# - You can chose whether or not to attempt to split WARC records into
#   headers and content blocks.
#
# - You can chose whether to cache certain things, for instance, the bytes
#   of headers or content blocks, in RAM on the parser object as it goes along,
#   or whether to read those bytes lazily on access.
#
# - Etc. More docs coming here, once I learn how to make automatic docs from
#   docstrings. But in the meantime, there's not that much code to read, if
#   you want to learn what an option does (feedback on clarity is welcome!)
```

[👆 Back to the summary](#summary)

---

## Development

### Linting

`uv run ruff check`
`uv run ruff format`

### Tests

`uv run pytest`

[👆 Back to the summary](#summary)

