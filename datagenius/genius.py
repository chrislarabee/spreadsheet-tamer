import inspect
import re
import collections as col
import functools
from abc import ABC
import typing

import datagenius.element as e
import datagenius.util as u


def parser(func=None, *tags,
           null_val=None,
           parses: str = 'row',
           requires_format: str = 'dicts',
           condition: (str, None) = None,
           priority: int = 10):
    """
    Acts as a wrapper for other functions so that functions passed
    to Genius.loop_rows have all the necessary attributes for
    successfully managing the loop.

    Args:
        func: A callable object.
        tags: Any number of strings which are valid tags to change
            how the parser is handled by Genius objects. Valid tags
            currently include:
                breaks_loop: Indicates that when the parser
                    successfully executes in a loop, the loop should
                    break.
                collect_rejects: Indicates that this parser's rejected
                    rows should be collected in the Dataset's rejects
                    attribute.
                uses_cache: Indicates this parser needs to reference the
                    previous result of the parser. Cannot be included
                    if parses is not 'row'.
                uses_meta_data: Indicates this parser needs to reference
                    the meta_data attribute of the Dataset.
        null_val: Leave this as None unless you need your parser
            to return None on a successful execution.
        parses: A string, indicates whether this parser expects to
            receive a 'row' of a Dataset, a 'column' of a Dataset, or
            'set' for the entire Dataset.
        requires_format: A string, indicates what format this
            parser needs the Dataset to be in to process it.
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
        wrapper_parser.args = inspect.getfullargspec(_func).args
        wrapper_parser.null_val = null_val
        valid_parses = ['row', 'column', 'set']
        if parses not in valid_parses:
            raise ValueError(
                f'Invalid parses passed: {parses}. Must pass one of '
                f'{valid_parses}')
        else:
            wrapper_parser.parses = parses
        wrapper_parser.requires_format = requires_format
        wrapper_parser.condition = condition
        wrapper_parser.priority = priority
        # Allocate tags:
        valid_tags = dict(
            uses_cache='uses',
            uses_meta_data='uses',
            collect_rejects='tag',
            breaks_loop='tag'
        )
        wrapper_parser.uses = []
        wrapper_parser.breaks_loop = False
        wrapper_parser.collect_rejects = False
        for t in tags:
            if t in valid_tags.keys():
                if valid_tags[t] == 'uses':
                    if t == 'uses_cache' and parses == 'set':
                        raise ValueError('Set parsers cannot use cache.')
                    wrapper_parser.uses.append(t[5:])
                else:
                    wrapper_parser.__dict__[t] = True
            else:
                raise ValueError(
                    f'{t} is not a valid tag. Valid tags include '
                    f'{valid_tags}'
                )
        wrapper_parser.is_parser = True
        return wrapper_parser
    # Allows parser to be used without arguments:
    if not isinstance(func, typing.Callable):
        tags = [func, *tags]
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
        parses = set()
        for s in steps:
            if u.validate_parser(s):
                parses.add(s.parses)
                formats.add(s.requires_format)
                results.append(s)
            else:
                raise ValueError(
                    f'ParserSubset objects only take parser functions. '
                    f'Invalid object={s}')
        msg = ('ParserSubset parsers must all have the same value for '
               '{0}. {0} = {1}')
        if len(formats) > 1:
            raise ValueError(msg.format('requires_format', formats))
        if len(parses) > 1:
            raise ValueError(msg.format('parses', parses))
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
                        for loop_rows's use (see loop_rows
                        for more info).

        Returns: The Dataset or a copy of it.

        """
        if options.get('overwrite', True):
            wdset = dset
        else:
            wdset = dset.copy()

        for step in self.steps:
            if u.validate_parser(step, 'parses', 'set'):
                step(wdset)
            else:
                if u.validate_parser(step):
                    s = [step]
                else:
                    s = step
                if u.validate_parser(step, 'parses', 'column'):
                    wdset.data = self.loop_columns(wdset, *s, **options)
                else:
                    wdset.data = self.loop_rows(wdset, *s, **options)
        return wdset

    @staticmethod
    def apply_parsers(x: (list, col.OrderedDict), *parsers, **parser_args):
        """
        Applies an arbitrary number of parsers to an object and returns the
        results.

        Args:
           x: A list or OrderedDict.
            *parsers: Any number of parser functions.
            **parser_args: Any number of kwargs. Keys that match the
                keyword arguments used by the parsers will be passed to
                them.

        Returns: A tuple containing the following:
            A boolean indicating whether to break any loop that
                apply parsers is in:
            A boolean indicating whether x passed through all the
                parsers successfully.
            A boolean indicating whether, if x did not pass through all
                the parsers successfully, it should be collected in
                its parent Dataset's rejects set.
            And finally, x.

        """
        cache = parser_args.get('cache')
        meta_data = parser_args.get('meta_data')
        _break = False
        passes_all = True
        collect_reject = False
        for p in parsers:
            if Genius.eval_condition(x, p.condition):
                if p.collect_rejects:
                    collect_reject = True
                _break = p.breaks_loop
                p_args = {k: v for k, v in parser_args.items() if k in p.args}
                if 'cache' in p.uses and cache is not None:
                    p_args['cache'] = cache
                if 'meta_data' in p.uses and meta_data not in (None, {}):
                    p_args['meta_data'] = meta_data
                parse_result = p(x, **p_args)
                if parse_result != p.null_val:
                    x = parse_result
                    if _break:
                        break
                else:
                    passes_all = False
        return _break, passes_all, collect_reject, x

    @staticmethod
    def align_dset_format(dset: e.Dataset, _format: str = 'dicts'):
        if dset.format != _format:
            dset.to_format(_format)

    @staticmethod
    def loop_rows(dset: e.Dataset, *parsers, one_return: bool = False,
                  **parser_args) -> (list or None):
        """
        Loops over all the rows in the passed Dataset and passes
        each to the passed parsers.

        Args:
            dset: A Dataset object.
            parsers: One or more parser functions.
            one_return: A boolean, tells loop_rows that the parsers
                will only result in a single object to return, so no
                need to wrap it in an outer list.
            parser_args: A dictionary containing keys matching the
                names of any of the parser functions, with each key
                being assigned a kwargs dictionary that the parser
                function can accept.

        Returns: A list containing the results of the parsers'
            evaluation of each row in dset.

        """
        results = []
        # loop_rows can change the Datasets format using the format
        # of the first parser in parsers if required:
        Genius.align_dset_format(dset, parsers[0].requires_format)

        parser_args['cache'] = None
        parser_args['meta_data'] = dset.meta_data

        for i in dset:
            row = i.copy()
            outer_break, passes_all, collect, row = Genius.apply_parsers(
                row, *parsers, **parser_args
            )
            if collect and not passes_all:
                dset.rejects.append(row)
            if passes_all:
                results.append(row)
                if outer_break:
                    break
                parser_args['cache'] = row

        if one_return:
            return results[0] if len(results) > 0 else None
        else:
            return results

    @staticmethod
    def get_column(dset: e.Dataset, column: (str, int)) -> list:
        """
        Gathers all the values in a given column of a Dataset.

        Args:
            dset: A Dataset object.
            column: A string, a value in dset.header, or an integer, a
                column index.

        Returns: A list of values from the column.

        """
        _format = 'lists' if isinstance(column, int) else 'dicts'
        print(dset[0])
        return Genius.loop_rows(
            dset,
            parser(lambda x: x[column], requires_format=_format)
        )

    @staticmethod
    def loop_columns(dset: e.Dataset, *parsers, **parser_args) -> (list or None):
        results = []

        # loop_columns can change the Datasets format using the format
        # of the first parser in parsers if required:
        Genius.align_dset_format(dset, parsers[0].requires_format)

        parser_args['cache'] = None
        parser_args['meta_data'] = dset.meta_data

        for v in dset.header:
            print(v, isinstance(v, str))
            column = Genius.get_column(dset, v)
            parser_args['col_name'] = v
            outer_break, passes_all, collect, row = Genius.apply_parsers(
                column, *parsers, **parser_args
            )
            if passes_all:
                results.append(column)
                if outer_break:
                    break
                parser_args['cache'] = column
        return results

    @staticmethod
    def eval_condition(data: (list, col.OrderedDict),
                       c: (str, None)) -> bool:
        """
        Takes a string formatted as a python conditional, with the
        antecedent being an index/key found in row, and evaluates
        if the value found at that location meets the condition
        or not.

        *** USE INNER QUOTES ON STRINGS WITHIN c! ***

        Args:
            data: A list or OrderedDict.
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
                if isinstance(data, list):
                    i = int(i)
                antecedent = data[i]
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
            wdset.header = self.loop_rows(
                wdset,
                options.get('header_func', self.detect_header),
                one_return=True
            )
            if wdset.header is not None:
                wdset.remove(wdset.header)
        return wdset

    @staticmethod
    @parser('collect_rejects', requires_format='lists')
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
        w = len(x) if threshold is None else threshold
        nn = u.non_null_count(x)
        return x if nn >= w else None

    @staticmethod
    @parser('breaks_loop', requires_format='lists')
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

    # TODO: Add a parser to convert '' to None.


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
        if options.get('extrapolate'):
            options['cols'] = options.get('extrapolate')
            self.steps.append(self.extrapolate)
        self.steps = self.order_parsers(self.steps)
        return super(Clean, self).go(dset, **options)

    @staticmethod
    @parser('uses_cache')
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
    @parser('uses_meta_data')
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

        explore_steps = [
            self.uniques_report,
            self.types_report,
            *custom_steps
        ]
        super(Explore, self).__init__(*self.order_parsers(explore_steps))

    def go(self, dset: e.Dataset, **options) -> e.Dataset:
        """
        Executes the explore steps on the Dataset.

        Args:
            dset: A Dataset object.
            **options: Keywords for customizing the functionality of go.
                Currently in use keywords:

        Returns: The Dataset object, or a copy of it.
        """
        return super(Explore, self).go(dset, **options)

    @staticmethod
    def nulls_report(column: list) -> dict:
        return {
            'null_ct': sum([1 if val in ('', None) else 0 for val in column])
        }

    @staticmethod
    @parser('uses_meta_data', parses='column')
    def types_report(column: list, col_name: str, meta_data: e.MetaData) -> list:
        """
        Takes a list and creates a dictionary report on the types of
        data found in the list and uses it to update meta_data.

        Args:
            column: A list.
            col_name: A string indicating the name of the column this
                data came from.
            meta_data: A MetaData object.

        Returns: column

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
        if value_ct > 0:
            str_pct = round((value_ct - type_sum) / value_ct, 2)
            num_pct = round(type_sum / value_ct, 2)
        else:
            str_pct = 0
            num_pct = 0
        if num_pct > str_pct:
            prob_type = 'numeric'
        elif str_pct > num_pct:
            prob_type = 'string'
        else:
            prob_type = 'uncertain'
        meta_data.update(
            col_name, str_pct=str_pct, num_pct=num_pct, probable_type=prob_type)
        return column

    @staticmethod
    @parser('uses_meta_data', parses='column')
    def uniques_report(column: list, col_name: str, meta_data: e.MetaData) -> list:
        """
        Takes a list and creates a dictionary report on the unique
        values of data found in the list and uses it to update
        meta_data.

        Args:
            column: A list.
            col_name: A string indicating the name of the column this
                data came from.
            meta_data: A MetaData object.

        Returns: column

        """
        uniques = set(column)
        unique_ct = len(uniques)
        if len(uniques) == len(column):
            unique_vals = 'primary_key'
        else:
            unique_vals = uniques
        meta_data.update(
            col_name, unique_ct=unique_ct, unique_values=unique_vals)
        return column
