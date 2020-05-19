import re
import collections as col
import functools
from abc import ABC

import datagenius.element as e
import datagenius.util as u


def parser(func=None, *,
           breaks_loop: bool = False,
           null_val=None,
           set_parser: bool = False,
           requires_format: str = 'dicts',
           takes_args: bool = False,
           uses_cache: bool = False,
           uses_meta_data: bool = False,
           condition: (str, None) = None,
           priority: int = 10):
    """
    Acts as a wrapper for other functions so that functions passed
    to Genius.loop_dataset have all the necessary attributes for
    successfully managing the loop.

    Args:
        func: A callable object.
        breaks_loop: A boolean, indicates that when the parser
            successfully executes in a loop, the loop should break.
        null_val: Leave this as None unless you need your parser
            to return None on a successful execution.
        set_parser: A boolean, indicates whether this parser should be
            run on the full dataset and not with a loop over the rows.
        requires_format: A string, indicates what format this
            parser needs the Dataset to be in to process it.
        takes_args: A boolean, indicates whether this parser can
            be run with arguments beyond one positional argument.
        uses_cache: A boolean, indicates whether this parser needs
            to reference the previous result of the parser to
            execute successfully. Cannot be True if set_parser
            is also True.
        uses_meta_data: A boolean, indicates wehther this parser needs
            to reference the meta_data attribute of the dataset.
        condition: A string in the format of a python conditional,
            with the antecedent of the conditional being a key
            or index that the parser function can find in the
            data it is passed.
        priority: An integer indicating that this parser needs
            to be executed at a specific time in a Genius object's
            parser execution plan. Parsers at the same priority
            will be placed in the plan in the order they are
            passed to the Genius object on instantiation.

    Returns: Passed func, but decorated.

    """
    # Allows parser functions to have state tracking:
    def decorator_parser(_func):
        # Allows parser to be used as a decorator.
        @functools.wraps(_func)
        def wrapper_parser(*args, **kwargs):
            return _func(*args, **kwargs)
        # Attributes of parser functions expected by other objects:
        wrapper_parser.breaks_loop = breaks_loop
        wrapper_parser.null_val = null_val
        wrapper_parser.set_parser = set_parser
        wrapper_parser.requires_format = requires_format
        wrapper_parser.takes_args = takes_args
        wrapper_parser.condition = condition
        wrapper_parser.priority = priority
        wrapper_parser.uses_meta_data = uses_meta_data
        if set_parser and uses_cache:
            raise ValueError('set_parsers cannot use cache.')
        else:
            wrapper_parser.uses_cache = uses_cache
        wrapper_parser.is_parser = True
        return wrapper_parser
    # Allows parser to be used without arguments:
    if func is None:
        return decorator_parser
    else:
        return decorator_parser(func)


class ParserSubset(col.abc.MutableSequence, ABC):
    """
    If a single Genius needs to take a subset of parsers, use a
    ParserSubset to group them and ensure they can still be properly
    ordered among the Genius' other parsers.
    """
    def __init__(self, *data, priority: int = 10):
        """

        Args:
            *data: Any number of parser functions.
            priority: An integer, indicates the priority this subset of
                parsers should take in a Genius object's parsing order.
        """
        self.data = self.validate_steps(data)
        self.priority = priority

    @staticmethod
    def validate_steps(steps: tuple):
        """
        Ensures that the passed tuple of steps are all
        parser functions, and that any sets of steps all expect
        the same format for the Dataset they will process.

        Args:
            steps: A tuple of parser functions.

        Returns: steps if they are all valid.

        """
        results = []
        formats = set()
        for s in steps:
            if u.validate_parser(s):
                formats.add(s.requires_format)
                results.append(s)
            else:
                raise ValueError(
                    f'ParserSubset objects only take parser functions. '
                    f'Invalid object={s}')
        if len(formats) > 1:
            raise ValueError(
                f'ParserSubset parsers must all have the same value '
                f'for requires_format. requires_formats = {formats}'
            )
        return results

    def insert(self, key: int, value: parser):
        self.data.insert(key, value)

    def __delitem__(self, key: int):
        self.data.remove(key)

    def __getitem__(self, item: int):
        return self.data[item]

    def __len__(self):
        return len(self.data)

    def __setitem__(self, key: int, value: parser):
        self.data[key] = value


class Genius:
    """
    The base class for pre-built and custom genius objects.
    Provides methods and attributes to assist in creating
    transforms that are as smart and flexible as possible.
    """
    def __init__(self, *steps):
        """

        Args:
            *steps: Any number of parser functions, or
                lists/tuples of parser functions that should
                be executed as a group.
        """
        self.steps = self.validate_steps(steps)

    @staticmethod
    def validate_steps(steps: tuple):
        """
        Ensures that the passed tuple of steps are all
        parser functions, and that any sets of steps all expect
        the same format for the Dataset they will process.

        Args:
            steps: A tuple of parser functions.

        Returns: steps if they are all valid.

        """
        results = []
        for s in steps:
            if hasattr(s, 'priority'):
                results.append(s)
            elif isinstance(s, (list, tuple)):
                raise ValueError(
                    f'If you are trying to use a subset of parsers pass '
                    f'a ParserSubset object instead of a list/tuple. '
                    f'Invalid step={s}.'
                )
            else:
                raise ValueError(
                    f'Genius objects only take parser functions or '
                    f'ParserSubsets as steps. Invalid step={s}')
        return results

    @staticmethod
    def order_parsers(parsers: (list, tuple)):
        """
        Places a list/tuple of parsers in priority order.
        Primarily used by objects that inherit from Genius and
        which need to mix built in parsers with user-defined
        parsers.

        Args:
            parsers: A list/tuple of parser functions.

        Returns: The list of parsers in increasing priority
            order.

        """
        if len(parsers) > 0:
            result = [parsers[0]]
            if len(parsers) > 1:
                for p in parsers[1:]:
                    idx = -1
                    for j, r in enumerate(result):
                        if p.priority > r.priority:
                            idx = j
                            break
                    if idx >= 0:
                        result.insert(idx, p)
                    else:
                        result.append(p)
            return result
        else:
            return parsers

    def go(self, dset: e.Dataset, **options) -> e.Dataset:
        """
        Runs the parser functions found on the Genius object
        in order on the passed Dataset.

        Args:
            dset: A Dataset object.
            **options: Keywords for customizing the functionality
                of go. Currently in use options:
                    overwrite: A boolean, tells go whether to
                        overwrite the contents of dset with the
                        results of the loops.
                    parser_args: A dictionary containing parser_args
                        for loop_dataset's use (see loop_dataset
                        for more info).

        Returns: The Dataset or a copy of it.

        """
        if options.get('overwrite', True):
            wdset = dset
        else:
            wdset = dset.copy()
        for step in self.steps:
            if u.validate_parser(step, 'set_parser'):
                step(wdset)
            else:
                if u.validate_parser(step):
                    s = [step]
                else:
                    s = step
                wdset.data = self.loop_dataset(
                    wdset, *s,
                    parser_args=options.get('parser_args')
                )
        return wdset

    @staticmethod
    def loop_dataset(dset: e.Dataset, *parsers, one_return: bool = False,
                     parser_args: dict = None) -> (list or None):
        """
        Loops over all the rows in the passed Dataset and passes
        each to the passed parsers.

        Args:
            dset: A Dataset object.
            parsers: One or more parser functions.
            one_return: A boolean, tells loop_dataset that the
                parsers will only result in a single
                object to return, so no need to wrap it
                in an outer list.
            parser_args: A dictionary containing keys matching
                the names of any of the parser functions, with
                each key being assigned a kwargs dictionary that
                the parser function can accept.

        Returns: A list containing the results of the parsers'
            evaluation of each row in dset.

        """
        results = []
        cache = None
        # loop_dataset can change the Datasets format using the format
        # of the first parser in parsers if required:
        first_format = parsers[0].requires_format
        if dset.format != first_format:
            dset.to_format(first_format)

        for i in dset:
            row = i.copy()
            passes_all = True
            # Used to break the outer loop too if a breaks_loop
            # parser evaluates successfully:
            outer_break = False
            for p in parsers:
                if Genius.eval_condition(row, p.condition):
                    if parser_args and p.takes_args:
                        p_args = parser_args.get(p.__name__)
                    else:
                        p_args = dict()
                    if p.uses_cache and cache is not None:
                        p_args['cache'] = cache
                    if p.uses_meta_data:
                        p_args['meta_data'] = dset.meta_data
                    parse_result = p(row, **p_args)
                    if parse_result != p.null_val:
                        row = parse_result
                        if p.breaks_loop:
                            outer_break = p.breaks_loop
                            break
                    else:
                        passes_all = False
            if passes_all:
                results.append(row)
                if outer_break:
                    break
                cache = row
        if one_return:
            if len(results) > 0:
                return results[0]
            else:
                return None
        else:
            return results

    @staticmethod
    def get_column(dset: e.Dataset, column: str) -> list:
        """
        Gathers all the values in a given column of a Dataset.

        Args:
            dset: A Dataset object.
            column: A string, a value in dset.header.

        Returns: A list of values from the column.

        """
        return Genius.loop_dataset(
            dset,
            parser(lambda x: x[column])
        )

    @staticmethod
    def eval_condition(row: (list, col.OrderedDict),
                       c: (str, None)) -> bool:
        """
        Takes a string formatted as a python conditional, with the
        antecedent being an index/key found in row, and evaluates
        if the value found at that location meets the condition
        or not.

        *** USE INNER QUOTES ON STRINGS WITHIN c! ***

        Args:
            row: A list or OrderedDict.
            c: A string or None, which must be a python
                conditional statement like "'key' == 'value'".

        Returns: A boolean.

        """
        if c is None:
            return True
        else:
            # First, handle strings contained within the c string:
            quotes = ["'", '"']
            consequent = None
            for q in quotes:
                quote_str = re.search(f'{q}.+{q}', c)
                if quote_str is not None:
                    consequent = quote_str.group()
                    c = c[:quote_str.start()]
                    break
            # Now it's safe to split it:
            components = c.split(' ')
            if consequent is not None:
                components[2] = consequent
            if len(components) > 3:
                raise ValueError(
                    f'"{c}" is not a valid conditional')
            else:
                # Get key/index:
                i = components[0]
                # Make sure i is the proper data type for row's
                # data type:
                if isinstance(row, list):
                    i = int(i)
                antecedent = row[i]
                # Make val a string that will pass eval:
                if isinstance(antecedent, str):
                    antecedent = '"' + antecedent + '"'
                else:
                    antecedent = str(antecedent)
                components[0] = antecedent
                condition = ' '.join(components)
            return eval(condition)

    def display_execute_plan(self) -> None:
        """
        Prints the step names in the Genius' steps attribute so the
        end-user can QA prioritization.

        Returns: None

        """
        print('priority\tstep name')
        for s in self.steps:
            print(f'{s.priority}\t\t\t{s.__name__}')

# TODO: Add a Genius object that can deal with excel workbooks
#       that have multiple sheets.


class Preprocess(Genius):
    """
    A Genius designed to clean up data that isn't ideally formatted,
    such as spreadsheets with gaps or other formatting that was
    designed for humans and not computers.
    """
    def __init__(self, *custom_steps):
        """

        Args:
            *custom_steps: Any number of parser functions, which
                will be executed after Preprocess' pre-built
                parsers.
        """
        preprocess_steps = [
            self.cleanse_gap,
            *custom_steps
        ]
        super(Preprocess, self).__init__(
            *preprocess_steps)

    def go(self, dset: e.Dataset, **options) -> e.Dataset:
        """
        Executes the preprocessing steps on the Dataset and then
        ensures the Dataset has a header.

        Args:
            dset: A Dataset object.
            **options: Keywords for customizing the functionality
                of go. Currently in use keywords:
                    manual_header: A list. Use this when your
                        data doesn't have a header and you are
                        manually creating one.
                    header_func: A parser, used if you need to
                        overwrite the default detect_header parser.
        Returns: The Dataset object, or a copy of it.

        """
        wdset = super(Preprocess, self).go(dset, **options)
        if options.get('manual_header'):
            wdset.header = options.get('manual_header')
        else:
            wdset.header = self.loop_dataset(
                wdset,
                options.get('header_func', self.detect_header),
                one_return=True
            )
            if wdset.header is not None:
                wdset.remove(wdset.header)
        return wdset

    @staticmethod
    @parser(requires_format='lists', takes_args=True)
    def cleanse_gap(x: list, threshold: int = None):
        """
        Checks a list to see if it has sufficient non-null values.

        Args:
            x: A list.
            threshold: An integer, indicates the number of # of
                values in the list that must be non-nulls. If None,
                uses the length of the list.

        Returns: The list if it contains equal to or greater
            non-null values than the threshold, otherwise None.

        """
        if threshold is None:
            w = len(x)
        else:
            w = threshold
        nn = u.non_null_count(x)
        if nn >= w:
            return x
        else:
            return None

    @staticmethod
    @parser(requires_format='lists', breaks_loop=True)
    def detect_header(x: list):
        """
        Checks a list to see if it contains only strings. If it
        does, then it could probably be a header row.

        Args:
            x: A list

        Returns: The list if it contains only non-null strings,
            otherwise None.

        """
        w = len(x)
        ts = u.true_str_count(x)
        if ts == w:
            return x
        else:
            return None


class Clean(Genius):
    """
    A Genius designed to clean up typos, type errors, and basically
    any other bad data entry in a Preprocessed Dataset.
    """
    def __init__(self, *custom_steps):
        """

        Args:
            *custom_steps: Any number of parser functions or
                ParserSubsets.
        """
        super(Clean, self).__init__(*self.order_parsers(custom_steps))

    def go(self, dset: e.Dataset, **options) -> e.Dataset:
        """
        Executes the clean steps on the Dataset.

        Args:
            dset: A Dataset object.
            **options: Keywords for customizing the functionality of go.
                Currently in use keywords:
                    extrapolate: A list/tuple of strings corresponding
                        to columns iin the Dataset, which will trigger
                        Clean to add its extrapolate parser to its
                        steps and pass the strings in extrapolate to it.

        Returns: The Dataset object, or a copy of it.

        """
        parser_args = dict()
        if options.get('extrapolate'):
            parser_args['extrapolate'] = {'cols': options.get('extrapolate')}
            self.steps.append(self.extrapolate)
        options = {**options, 'parser_args': parser_args}
        self.steps = self.order_parsers(self.steps)
        return super(Clean, self).go(dset, **options)

    @staticmethod
    @parser(takes_args=True, uses_cache=True)
    def extrapolate(x: col.OrderedDict, cols: (list, tuple),
                    cache: col.OrderedDict = None):
        """
        Uses the values in a cached row to fill in values in the current
        row by index. Useful when your dataset has grouped rows.

        Args:
            x: An OrderedDict.
            cols: A list of keys, which must be found in x.
            cache: An OrderedDict, which contains values to be
                pulled by key in cols into x. If cache is None,
                extrapolate will just return a copy of x.

        Returns: x with null values overwritten with populated
            values from the cached OrderedDict.

        """
        result = x.copy()
        if cache is not None:
            for c in cols:
                if result[c] in (None, ''):
                    result[c] = cache[c]
        return result

    @staticmethod
    @parser(uses_meta_data=True)
    def clean_typos(x: dict, meta_data: dict):
        typo_funcs = {
            'numeric': Clean.clean_numeric_typos
        }
        result = dict()
        for k, v in x.items():
            f = typo_funcs.get(
                meta_data[k]['probable_type'],
                lambda y: y
            )
            result[k] = f(v)
        return result

    @staticmethod
    def clean_numeric_typos(value: str) -> (float, str):
        """
        Attempts to turn a string which might be a number with typos in
        it into a number. Should only be used on columns that you are
        confident *should* be entirely numbers, as it will remove
        any non-numerals or periods from the passed string

        Args:
            value: A string.

        Returns: A float or the string.

        """
        result = value
        if not result.isnumeric():
            result = result.replace(',', '.')
            result = ''.join(re.findall(r'[0-9]+|\.', result))
            try:
                result = float(result)
            except ValueError:
                result = value
        return result


class Explore(Genius):
    """
    A Genius designed to create meta_data for a Dataset and help guide
    creation of Clean steps.
    """
    def __init__(self, *custom_steps):
        """

        Args:
            *custom_steps: Any number of functions, which must take a
                single list argument.
        """

        self.report_steps = [
            self.uniques_report,
            self.types_report,
            *custom_steps
        ]
        super(Explore, self).__init__()

    def go(self, dset: e.Dataset, **options) -> e.Dataset:
        """
        Executes the explore steps on the Dataset.

        Args:
            dset: A Dataset object.
            **options: Keywords for customizing the functionality of go.
                Currently in use keywords:

        Returns: The Dataset object, or a copy of it.
        """
        for v in dset.header:
            column = self.get_column(dset, v)
            for rs in self.report_steps:
                dset.update_meta_data(v, **rs(column))
        return dset

    @staticmethod
    def types_report(column: list) -> dict:
        """
        Takes a list and creates a dictionary report on the types of
        data found in the list.

        Args:
            column: A list.

        Returns: A dictionary of meta_data about the list's data types.

        """
        types = []
        for val in column:
            if isinstance(val, (float, int)):
                types.append(1)
            elif isinstance(val, str):
                types.append(1 if val.isnumeric() else 0)
            else:
                types.append(0)
        type_sum = sum(types)
        value_ct = len(column)
        str_pct = round((value_ct - type_sum) / value_ct, 2)
        num_pct = round(type_sum / value_ct, 2)
        if num_pct > str_pct:
            prob_type = 'numeric'
        elif str_pct > num_pct:
            prob_type = 'string'
        else:
            prob_type = 'uncertain'
        return {
            'str_pct': str_pct,
            'num_pct': num_pct,
            'probable_type': prob_type
        }

    @staticmethod
    def uniques_report(column: list) -> dict:
        """
        Takes a list and creates a dictionary report on the unique
        values of data found in the list.

        Args:
            column: A list.

        Returns: A dictionary of meta_data about the list's unique
            values.

        """
        uniques = set(column)
        unique_ct = len(uniques)
        if len(uniques) == len(column):
            unique_vals = 'primary_key'
        else:
            unique_vals = uniques
        return {
            'unique_ct': unique_ct,
            'unique_values': unique_vals
        }
