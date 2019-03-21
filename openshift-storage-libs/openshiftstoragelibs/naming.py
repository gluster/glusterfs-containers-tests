"""Helper functions for working with names for volumes, resources, etc.
"""

import string
import random
import re

# we only use lowercase here because kubernetes requires
# names to be lowercase or digits, so that is our default
try:
    # py2
    UNIQUE_CHARS = (string.lowercase + string.digits)
except AttributeError:
    # py3
    UNIQUE_CHARS = (string.ascii_lowercase + string.digits)


def make_unique_label(prefix=None, suffix=None, sep='-',
                      clean=r'[^a-zA-Z0-9]+', unique_len=8,
                      unique_chars=UNIQUE_CHARS):
    """Generate a unique name string based on an optional prefix,
    suffix, and pseudo-random set of alphanumeric characters.

    Args:
        prefix (str): Start of the unique string.
        suffix (str): End of the unique string.
        sep (str): Separator string (between sections/invalid chars).
        clean (str): Reqular expression matching invalid chars.
            that will be replaced by `sep` if found in the prefix or suffix
        unique_len (int): Length of the unique part.
        unique_chars (str): String representing the set of characters
            the unique part will draw from.
    Returns:
        str: The uniqueish string.
    """
    cre = re.compile(clean)
    parts = []
    if prefix:
        parts.append(cre.sub(sep, prefix))
    parts.append(''.join(random.choice(unique_chars)
                         for _ in range(unique_len)))
    if suffix:
        parts.append(cre.sub(sep, suffix))
    return sep.join(parts)


def extract_method_name(full_name, keep_class=False):
    """Given a full test name as returned from TestCase.id() return
    just the method part or class.method.

    Args:
        full_name (str): Dot separated name of test.
        keep_class (str): Retain the class name, if false only the
            method name will be returned.
    Returns:
        str: Method name or class.method_name.
    """
    offset = -1
    if keep_class:
        offset = -2
    return '.'.join(full_name.split('.')[offset:])
