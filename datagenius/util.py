

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


def validate_parser(f, attr: str = 'is_parser', match=True) -> bool:
    """
    Takes an object and checks its attributes. Designed to see
    if a given function has been decorated as a parser and what '
    its parser attributes are. The additional arguments are really
    only necessary in situations where you need to check an
    parser's parser attributes without first checking if it's
    actually a parser or not.

    Args:
        f: Any object.
        attr: A string, the attribute to check against. Defaults
            to the is_parser attribute.
        match: The value to check attr against. Defaults to true.

    Returns: A boolean indicating whether the object has the passed
        attribute and if it matches the passed match.

    """
    result = False
    if attr in f.__dict__.keys():
        if f.__getattribute__(attr) == match:
            result = True
    return result
