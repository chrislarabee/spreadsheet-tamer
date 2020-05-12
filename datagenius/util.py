

def non_null_count(x: list) -> int:
    """
    Takes a list and returns the number of values in it that are
    not None or ''.

    Args:
        x: A list.

    Returns: An integer, the count of non-nulls in the list.

    """
    return sum([0 if y in (None, '') else 1 for y in x])


def true_str_count(x: list) -> int:
    """
    Takes a list and returns the number of values in it that are
    strings that are not ''.

    Args:
        x: A list.

    Returns: An integer, the count of non-blank strings.

    """
    return sum(
        [1 if isinstance(y, str) and y != '' else 0 for y in x]
    )
