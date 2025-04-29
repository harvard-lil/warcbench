__all__ = [
    "sample_custom_handler",
]


def sample_custom_handler(record):
    if record.header.get_field("WARC-Type", decode=True) == "response":
        with open("/tmp/custom-handler-report.txt", "a") as file:
            file.write(
                f"Offset: {record.start} Length: {record.length} URI: {record.header.get_field('WARC-Target-URI', decode=True)}\n"
            )
