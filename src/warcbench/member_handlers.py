"""
`member_handlers` module: Functions that return helper functions that take a GzippedMember and return None
"""


def get_member_offsets(compressed=True, append_to=None, print_each=True):
    def f(member):
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
