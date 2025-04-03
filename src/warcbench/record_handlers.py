"""
`record_handlers` module: Functions that return helper functions that take a Record and return None
"""


def get_record_offsets(split=False, append_to=None, print_each=True):
    def f(record):
        if split:
            offsets = (
                record.header.start,
                record.header.end,
                record.content_block.start,
                record.content_block.end,
            )
        else:
            offsets = (record.start, record.end)

        if append_to is not None:
            append_to.append(offsets)

        if print_each:
            if split:
                print(f"Record bytes {offsets[0]}-{offsets[3]}")
                print(f"Header bytes {offsets[0]}-{offsets[1]}")
                print(f"Content bytes {offsets[2]}-{offsets[3]}")
            else:
                print(f"Record bytes {offsets[0]}-{offsets[1]}")
            print()

    return f


def get_record_headers(decode_utf8=True, append_to=None, print_each=True):
    def f(record):
        if record.header:
            data = record.header.bytes
            if append_to is not None:
                if decode_utf8:
                    header = data.decode()
                else:
                    header = data
                append_to.append(header)

            if print_each:
                for header_line in data.split(b"\r\n"):
                    if decode_utf8:
                        line = header_line.decode()
                    else:
                        line = header_line
                    if line:
                        print(line)
                print()
        else:
            if append_to is not None:
                append_to.append(None)

            if print_each:
                print("Record header not parsed.")
                print()

    return f


def get_record_content(append_to=None, print_each=False):
    def f(record):
        if record.content_block:
            data = record.content_block.bytes
            if append_to is not None:
                append_to.append(data)

            if print_each:
                print(data)
                print()
        else:
            if append_to is not None:
                append_to.append(None)

            if print_each:
                print("Record content not parsed.")
                print()

    return f


def get_record_http_headers(decode="ascii", append_to=None, print_each=True):
    def f(record):
        header_block = record.get_http_header_block()
        if header_block:
            if append_to is not None:
                if decode:
                    headers = header_block.decode(decode)
                else:
                    headers = record.header
                append_to.append(headers)

            if print_each:
                for header_line in header_block.split(b"\r\n"):
                    if decode:
                        line = header_line.decode(decode)
                    else:
                        line = header_line
                    if line:
                        print(line)
                print()
        else:
            if append_to is not None:
                append_to.append(None)

            if print_each:
                if record.content_block:
                    print("No HTTP headers parsed.")
                else:
                    print("Record content not parsed.")
                print()

    return f


def get_record_http_body(append_to=None, print_each=False):
    def f(record):
        body = record.get_http_body_block()
        if body:
            if append_to is not None:
                append_to.append(body)

            if print_each:
                print(body)
                print()
        else:
            if append_to is not None:
                append_to.append(None)

            if print_each:
                if record.content_block:
                    print("No HTTP body parsed.")
                else:
                    print("Record content not parsed.")
                print()

    return f


def print_separator(separator="-" * 40):
    def f(record):
        print(separator)

    return f
