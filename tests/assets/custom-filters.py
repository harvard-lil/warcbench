__all__ = [
    "sample_custom_filter",
]


def sample_custom_filter(record):
    return (
        record.header.get_field("WARC-Type", decode=True) == "warcinfo"
        or b"Scoop-Exchange-ID: 5733be1f-60ea-47c8-99be-abc4f8b31846"
        in record.header.bytes
    )
