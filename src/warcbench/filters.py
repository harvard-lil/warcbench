"""
`filters` module: Functions that return helper functions that take a Record and return True/False
"""

# Standard library imports
import operator

# Warcbench imports
from warcbench.patterns import (
    CONTENT_LENGTH_PATTERN,
    CONTENT_TYPE_PATTERN,
    get_http_header_pattern,
    get_http_status_pattern,
    get_http_verb_pattern,
    get_warc_named_field_pattern,
)
from warcbench.utils import find_pattern_in_bytes, is_target_in_bytes

# Typing imports
from typing import Callable, Dict, Union, TYPE_CHECKING

if TYPE_CHECKING:
    from warcbench.models import Record


def warc_header_regex_filter(
    regex: str, case_insensitive: bool = True
) -> Callable[["Record"], bool]:
    """
    Finds WARC records with whose header bytes match the passed in regex.
    """

    def f(record: "Record") -> bool:
        return bool(
            find_pattern_in_bytes(
                bytes(regex, "utf-8"),
                record.header.bytes,  # type: ignore[union-attr]
                case_insensitive=case_insensitive,
            )
        )

    return f


def record_content_length_filter(
    target_length: int, use_operator: str = "eq"
) -> Callable[["Record"], bool]:
    """
    Finds WARC records with whose header includes a specified Content-Length
    that matches the target length. Available comparison operators:
    eq (default), lt, le, gt, ge, ne.
    """
    allowed_operators: Dict[str, Callable[[int, int], bool]] = {
        "lt": operator.lt,
        "le": operator.le,
        "eq": operator.eq,
        "ne": operator.ne,
        "gt": operator.gt,
        "ge": operator.ge,
    }
    if use_operator not in allowed_operators:
        raise ValueError(f"Supported operators: {', '.join(allowed_operators)}.")

    def f(record: "Record") -> bool:
        match = find_pattern_in_bytes(
            CONTENT_LENGTH_PATTERN,
            record.header.bytes,  # type: ignore[union-attr]
            case_insensitive=True,
        )

        if match:
            extracted = int(match.group(1))
            return allowed_operators[use_operator](extracted, target_length)
        else:
            return False

    return f


def record_content_type_filter(
    content_type: str, case_insensitive: bool = True, exact_match: bool = False
) -> Callable[["Record"], bool]:
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

    def f(record: "Record") -> bool:
        match = find_pattern_in_bytes(
            CONTENT_TYPE_PATTERN,
            record.header.bytes,  # type: ignore[union-attr]
            case_insensitive=case_insensitive,
        )
        if match:
            extracted = match.group(1)
            return is_target_in_bytes(
                extracted,
                content_type,
                case_insensitive=case_insensitive,
                exact_match=exact_match,
            )
        return False

    return f


def warc_named_field_filter(
    field_name: str,
    target: str,
    case_insensitive: bool = True,
    exact_match: bool = False,
) -> Callable[["Record"], bool]:
    """
    Finds WARC records with a named header field that matches the specified target
    http://iipc.github.io/warc-specifications/specifications/warc-format/warc-1.1/#named-fields
    """

    def f(record: "Record") -> bool:
        match = find_pattern_in_bytes(
            get_warc_named_field_pattern(field_name),
            record.header.bytes,  # type: ignore[union-attr]
            case_insensitive=case_insensitive,
        )
        if match:
            extracted = match.group(1)
            return is_target_in_bytes(
                extracted,
                target,
                case_insensitive=case_insensitive,
                exact_match=exact_match,
            )
        return False

    return f


def http_verb_filter(verb: str) -> Callable[["Record"], bool]:
    """
    Finds WARC records with a Content-Type of application/http; msgtype=request,
    then filters on HTTP verb.
    """

    def f(record: "Record") -> bool:
        if record_content_type_filter("msgtype=request")(record):
            http_headers = record.get_http_header_block()
            match = find_pattern_in_bytes(get_http_verb_pattern(verb), http_headers)  # type: ignore[arg-type]
            if match:
                extracted = match.group(1)
                return is_target_in_bytes(extracted, verb, exact_match=True)
        return False

    return f


def http_status_filter(status_code: Union[str, int]) -> Callable[["Record"], bool]:
    """
    Finds WARC records with a Content-Type of application/http; msgtype=response,
    then filters on HTTP status code.
    """

    def f(record: "Record") -> bool:
        if record_content_type_filter("msgtype=response")(record):
            http_headers = record.get_http_header_block()
            match = find_pattern_in_bytes(
                get_http_status_pattern(status_code),
                http_headers,  # type: ignore[arg-type]
            )
            if match:
                extracted = match.group(1)
                return is_target_in_bytes(extracted, str(status_code), exact_match=True)
        return False

    return f


def http_header_filter(
    header_name: str,
    target: str,
    case_insensitive: bool = True,
    exact_match: bool = False,
) -> Callable[["Record"], bool]:
    """
    Finds WARC records with a Content-Type that includes application/http,
    then filters on any HTTP header.
    """

    def f(record: "Record") -> bool:
        if record_content_type_filter("application/http")(record):
            http_headers = record.get_http_header_block()
            match = find_pattern_in_bytes(
                get_http_header_pattern(header_name),
                http_headers,  # type: ignore[arg-type]
                case_insensitive=case_insensitive,
            )
            if match:
                extracted = match.group(1)
                return is_target_in_bytes(
                    extracted,
                    target,
                    case_insensitive=case_insensitive,
                    exact_match=exact_match,
                )
        return False

    return f


def http_response_content_type_filter(
    content_type: str, case_insensitive: bool = True, exact_match: bool = False
) -> Callable[["Record"], bool]:
    """
    Finds WARC records with a Content-Type of application/http; msgtype=response,
    then filters on the HTTP header "Content-Type".
    """

    def f(record: "Record") -> bool:
        if record_content_type_filter("msgtype=response")(record):
            http_headers = record.get_http_header_block()
            match = find_pattern_in_bytes(
                CONTENT_TYPE_PATTERN,
                http_headers,  # type: ignore[arg-type]
                case_insensitive=case_insensitive,
            )
            if match:
                extracted = match.group(1)
                return is_target_in_bytes(
                    extracted,
                    content_type,
                    case_insensitive=case_insensitive,
                    exact_match=exact_match,
                )
        return False

    return f
