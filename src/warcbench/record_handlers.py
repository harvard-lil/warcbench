"""
`record_handlers` module: Functions that return helper functions that take a Record and return None
"""


def print_record_attribute(attribute):
    """This is just a demo"""

    def f(record):
        print(getattr(record, attribute))

    return f
