

def non_null_count(x: list) -> int:
    """
    Takes a list and returns the number of values in it that are
    not None or ''.

    Args:
        x: A list.

    Returns: An integer, the cont of non-nulls in the list.

    """
    return sum([0 if y in (None, '') else 1 for y in x])
