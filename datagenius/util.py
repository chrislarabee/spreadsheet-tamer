from collections import OrderedDict


def collect_by_keys(x: (dict, OrderedDict), *keys) -> (dict, OrderedDict):
    """
    A simple function to collect an arbitrary and not-necessarily
    ordered subset of a dictionary.

    Args:
        x: A dictionary or OrderedDict.
        *keys: An arbitrary list of keys that could be found in x.

    Returns: A dictionary or OrderedDict containing only the passed
        keys. Returns an object of the same type passed.

    """
    result = type(x)()
    for k, v in x.items():
        if k in keys:
            result[k] = v
    return result


def count_nulls(x: (list, OrderedDict, dict)) -> int:
    """
    Takes a list or dictionary and returns the number of values in it
    that are None or ''.

    Args:
        x: A list or dictionary.

    Returns: An integer, the count of non-nulls in the list.

    """
    x = list(x.values()) if isinstance(x, (OrderedDict, dict)) else x
    return sum([1 if y in (None, '') else 0 for y in x])


def count_true_str(x: list) -> int:
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


def nullify_empty_vals(x: (list, dict, OrderedDict),
                       *ignore) -> (list, dict, OrderedDict):
    """
    Takes a list, dict, or OrderedDict and ensures that any of its
    values that are empty strings are replaced by None, unless the
    index or key is in *ignore.
    Args:
        x: A list, dict, or OrderedDict.
        ignore: An arbitrary list of keys/indices to NOT nullify.

    Returns: An object of the same type as x.

    """
    indices = range(len(x)) if isinstance(x, list) else list(x.keys())
    result = [None if x[i] == '' and i not in ignore else x[i] for i in indices]
    if isinstance(x, (dict, OrderedDict)):
        return type(x)(zip(indices, result))
    else:
        return result


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
    if hasattr(f, attr):
        if getattr(f, attr) == match:
            result = True
    return result
