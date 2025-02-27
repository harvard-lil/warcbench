"""
"""

from warcbench.patterns import get_warc_named_field_pattern
from warcbench.utils import find_pattern_in_bytes, find_match_in_extracted_header

def warc_named_field_filter(field_name, target, case_insensitive=True, exact_match=False):
    def f(record):
        match = find_pattern_in_bytes(
            get_warc_named_field_pattern(field_name),
            record.header.bytes,
            case_insensitive=case_insensitive
        )
        if match:
            extracted = match.group(1)
            return is_target_in_bytes(
                extracted,
                target,
                case_insensitive=case_insensitive,
                exact_match=exact_match
            )
        return False
    return f


def warc_header_regex_filter(regex, case_insensitive=True):
    def f(record):
        return bool(
            find_pattern_in_bytes(
                bytes(regex, "utf-8"),
                record.header.bytes,
                case_insensitive=case_insensitive
            )
        )
    return f


def record_content_length_filter(target_length, use_operator="eq"):
    allowed_operators = {
        'lt': operator.lt,
        'le': operator.le,
        'eq': operator.eq,
        'ne': operator.ne,
        'gt': operator.gt,
        'ge': operator.ge,
    }
    if use_operator not in allowed_operators:
        raise ValueError(f"Supported operators: {', '.join(allowed_operators)}.")

    def f(record):
        match = find_pattern_in_bytes(CONTENT_LENGTH_PATTERN, record.header.bytes, case_insensitive=True)

        if match:
            extracted = int(match.group(1))
            return allowed_operators[use_operator](extracted, target_length)
        else:
            return False
    return f


def record_content_type_filter(content_type, case_insensitive=True, exact_match=False):
    """
    Filters on the Content-Type field of the WARC header.

    Expected values:
    - application/warc-fields
    - application/http; msgtype=request
    - application/http; msgtype=response
    - image/jpeg or another mime type, for resource records

    NB: This field does NOT refer to the content-type header of recorded HTTP responses.
    See `http_response_content_type_filter`.
    """
    def f(record):
        match = find_pattern_in_bytes(
            CONTENT_TYPE_PATTERN,
            record.header.bytes,
            case_insensitive=case_insensitive
        )
        if match:
            extracted = match.group(1)
            return is_target_in_bytes(
                extracted,
                content_type,
                case_insensitive=case_insensitive,
                exact_match=exact_match
            )
        return False
    return f


def http_verb_filter(verb):
    """
    Finds WARC records with a Content-Type of application/http; msgtype=request,
    then filters on HTTP verb.
    """
    def f(record):
        if record_content_type_filter('application/http; msgtype=request')(record):
            http_headers = record.get_http_header_block()
            match = find_pattern_in_bytes(
                get_http_verb_pattern(verb),
                http_headers
            )
            if match:
                extracted = match.group(1)
                return is_target_in_bytes(
                    extracted,
                    verb,
                    exact_match=True
                )
        return False
    return f


def http_status_filter(status_code):
    """
    Finds WARC records with a Content-Type of application/http; msgtype=response,
    then filters on HTTP status code.
    """
    def f(record):
        if record_content_type_filter('application/http; msgtype=response')(record):
            http_headers = record.get_http_header_block()
            match = find_pattern_in_bytes(
                get_http_status_pattern(status_code),
                http_headers
            )
            if match:
                extracted = match.group(1)
                return is_target_in_bytes(
                    extracted,
                    str(status_code),
                    exact_match=True
                )
        return False
    return f


def http_header_filter(header_name, target, case_insensitive=True, exact_match=False):
    """
    Finds WARC records with a Content-Type that includes application/http,
    then filters on any HTTP header.
    """
    def f(record):
        if record_content_type_filter('application/http')(record):
            http_headers = record.get_http_header_block()
            match = find_pattern_in_bytes(
                get_http_header_pattern(header_name),
                http_headers,
                case_insensitive=case_insensitive
            )
            if match:
                extracted = match.group(1)
                return is_target_in_bytes(
                    extracted,
                    target,
                    case_insensitive=case_insensitive,
                    exact_match=exact_match
                )
        return False
    return f


def http_response_content_type_filter(content_type, case_insensitive=True, exact_match=False):
    """
    Finds WARC records with a Content-Type of application/http; msgtype=response,
    then filters on the HTTP header "Content-Type".
    """
    def f(record):
        if record_content_type_filter('application/http; msgtype=response')(record):
            http_headers = record.get_http_header_block()
            match = find_pattern_in_bytes(
                CONTENT_TYPE_PATTERN,
                http_headers,
                case_insensitive=case_insensitive
            )
            if match:
                extracted = match.group(1)
                return is_target_in_bytes(
                    extracted,
                    content_type,
                    case_insensitive=case_insensitive,
                    exact_match=exact_match
                )
        return False
    return f
