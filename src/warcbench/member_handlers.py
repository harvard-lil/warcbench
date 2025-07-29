"""
`member_handlers` module: Functions that return helper functions that take a GzippedMember and return None
"""

# Warcbench imports
from warcbench.models import GzippedMember

# Typing imports
from typing import Callable, List, Optional, Tuple


def get_member_offsets(
    compressed: bool = True,
    append_to: Optional[List[Tuple[Optional[int], Optional[int]]]] = None,
    print_each: bool = True,
) -> Callable[[GzippedMember], None]:
    """
    A handler that extracts and optionally prints byte offsets of gzip members.

    This handler works specifically with GzippedMember objects from WARCGZParser.
    It can extract offsets either for the compressed gzip member boundaries
    or the equivalent positions in the decompressed data.

    Args:
        compressed: If True, extract offsets in the compressed file. If False,
            extract offsets as they would appear in the decompressed file.
        append_to: Optional list to append offset tuples to.
        print_each: If True, print offset information for each member.

    Returns:
        Callable[[GzippedMember], None]: A handler function that can be passed to
        WARCGZParser processors as a member_handler.

    Example:
        ```python
        compressed_offsets = []
        uncompressed_offsets = []

        handlers = [
            get_member_offsets(compressed=True, append_to=compressed_offsets, print_each=False),
            get_member_offsets(compressed=False, append_to=uncompressed_offsets, print_each=False)
        ]

        parser = WARCGZParser(
            file,
            processors=WARCGZProcessorConfig(member_handlers=handlers)
        )
        parser.parse()
        ```
    """

    def f(member: GzippedMember) -> None:
        offsets: Tuple[Optional[int], Optional[int]]
        if compressed:
            offsets = (member.start, member.end)
        else:
            offsets = (member.uncompressed_start, member.uncompressed_end)

        if append_to is not None:
            append_to.append(offsets)

        if print_each:
            print(
                f"Member bytes {offsets[0]}-{offsets[1]} ({'compressed' if compressed else 'uncompressed'})"
            )
            print()

    return f
