import inspect
import re
import collections as col
import functools
import warnings
from abc import ABC
from typing import Callable

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
    to Genius.loop_dataset have all the necessary attributes for
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
        null_val: Leave this as None unless you need your parser
            to return None on a successful execution.
        parses: A string, indicates whether this parser expects to
            receive a 'row' of a Dataset, a 'column' of a Dataset, or
            'set' for the entire Dataset.
        requires_format: A string, indicates what data_format this
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
        valid_tags = tuple([
            'collect_rejects',
            'breaks_loop',
        ])
        wrapper_parser.breaks_loop = False
        wrapper_parser.collect_rejects = False
        for t in tags:
            if t is None:
                pass
            elif t in valid_tags:
                wrapper_parser.__dict__[t] = True
            else:
                raise ValueError(
                    f'{t} is not a valid tag. Valid tags include '
                    f'{valid_tags}'
                )
        wrapper_parser.is_parser = True
        return wrapper_parser
    # Allows parser to be used without arguments:
    if not isinstance(func, Callable):
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
        self.priority: int = priority
        self.parses: (str, None) = None
        self.requires_format: (str, None) = None
        self.data, self.parses, self.requires_format = self.validate_steps(data)

    @staticmethod
    def validate_steps(steps: tuple):
        """
        Ensures that the passed tuple of steps are all parser
        functions, and that any sets of steps all expect the same
        data_format for the Dataset they will process.

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
                if s.requires_format != 'any':
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
        return results, list(parses)[0], list(formats)[0]

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
        the same data_format for the Dataset they will process.

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
            wdset.transpose(step.parses)
            s = [step] if u.validate_parser(step) else step
            if step.parses == 'set':
                self.loop_dataset(wdset, *s, **options)
            else:
                wdset._data = self.loop_dataset(wdset, *s, **options)
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
        _break = False
        passes_all = True
        collect_reject = False
        for p in parsers:
            if Genius.eval_condition(x, p.condition):
                if p.collect_rejects:
                    collect_reject = True
                _break = p.breaks_loop
                p_args = {k: v for k, v in parser_args.items() if k in p.args}
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
        if dset.data_format != _format:
            dset.to_format(_format)

    @staticmethod
    def loop_dataset(dset: e.Dataset, *parsers, **parser_args) -> (list or None):
        """
        Loops over all the rows in the passed Dataset and passes
        each to the passed parsers.

        Args:
            dset: A Dataset object.
            parsers: One or more parser functions.
            parser_args: A dictionary containing keys matching the
                args of any of the parser functions.

        Returns: A list containing the results of the parsers'
            evaluation of each row in dset.

        """
        results = []
        # loop_dataset can change the Datasets data_format using the
        # data_format of the first parser in parsers if required:
        Genius.align_dset_format(dset, parsers[0].requires_format)

        parser_args['cache'] = None
        parser_args['meta_data'] = dset.meta_data

        for i, r in enumerate(dset):
            if dset.data_orientation == 'column':
                parser_args['col_name'] = dset.meta_data.header[i]
            parser_args['index'] = i
            row = r.copy()
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
    def __init__(self, *custom_steps, header_func=None):
        """

        Args:
            *custom_steps: Any number of parser functions, which will
                be executed along with Preprocess' pre-built parsers.
            header_func: A parser function, overrides pre-built
                detect_header parser.
        """
        if u.validate_parser(header_func) and header_func.priority >= 100:
            warnings.warn(
                f'It is *highly* recommended that you let Preprocess '
                f'execute cleanse_gaps and nullify_empty_vals before '
                f'executing a header-finding function. Current '
                f'header_func priority = {header_func.priority}. '
                f'Consider reducing it below 100')
        preprocess_steps = [
            self.cleanse_gaps,
            self.nullify_empty_vals,
            self.detect_header if header_func is None else header_func,
            self.cleanse_pre_header,
            *custom_steps
        ]
        super(Preprocess, self).__init__(*self.order_parsers(preprocess_steps))

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
                    ignore: A tuple, a list of indices in your dataset
                        with meaningful empty strings that should NOT
                        be converted to NoneType.
        Returns: The Dataset object, or a copy of it.

        """
        wdset = super(Preprocess, self).go(dset, **options)
        if wdset.meta_data.header in wdset:
            wdset.remove(wdset.meta_data.header)
        return wdset

    @staticmethod
    @parser(requires_format='lists', priority=100)
    def cleanse_gaps(row: list) -> (None, list):
        """
        Checks a list to see if it contains only null values.

        Args:
            row: A list.

        Returns: None if the list contains only null values, else the
            list.

        """
        return None if u.count_nulls(row, strict=False) == len(row) else row

    @staticmethod
    @parser('breaks_loop', parses='set',
            requires_format='lists')
    def detect_header(row: list, meta_data: e.MetaData, index: (int, None) = None,
                      manual_header: (list, None) = None) -> (list, None):
        """
        Checks a list to see if it contains only strings. If it
        does, then it could probably be a header row.

        Args:
            row: A list.
            meta_data: A MetaData object.
            index: The index of the row in the Dataset it came from.
            manual_header: A list, which will be used to override any
                automatically detected header. Useful if the Dataset
                has no discernible header.

        Returns: The list if it contains only non-null strings,
            otherwise None.

        """
        if manual_header is not None:
            meta_data.header_idx = index
            meta_data.header = manual_header
            return row
        else:
            w = len(row)
            ts = u.count_true_str(row)
            if ts == w:
                meta_data.header_idx = index
                meta_data.header = row
                return row
            else:
                return None

    @staticmethod
    @parser(requires_format='any', priority=100)
    def nullify_empty_vals(
            row: (list, dict, col.OrderedDict),
            ignore: (tuple, None) = None) -> (list, dict, col.OrderedDict):
        """
        Takes a list, dict, or OrderedDict and ensures that any of its
        values that are empty strings are replaced by None, unless the
        index or key is in *ignore.
        Args:
            row: A list, dict, or OrderedDict.
            ignore: A tuple of keys/indices to NOT nullify.

        Returns: An object of the same type as row.

        """
        indices = range(len(row)) if isinstance(row, list) else list(row.keys())
        ignore = tuple() if ignore is None else ignore
        result = [
            None if row[i] == '' and i not in ignore else row[i] for i in indices
        ]
        if isinstance(row, (dict, col.OrderedDict)):
            return type(row)(zip(indices, result))
        else:
            return result

    @staticmethod
    @parser('collect_rejects', requires_format='lists', priority=9)
    def cleanse_pre_header(row: list, meta_data: e.MetaData,
                           index: (int, None)) -> (list, None):
        """
        Checks if a passed list's index came before the header's
        index. If it did, then the row will be rejected.

        Args:
            row: A list.
            meta_data: A meta_data object.
            index: The index of the row in the Dataset it came from.

        Returns: None if the index came before meta_Data.header_idx,
            otherwise the list.

        """
        if meta_data.header_idx is not None and index < meta_data.header_idx:
            return None
        else:
            return row


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
    @parser
    def extrapolate(row: col.OrderedDict, cols: (list, tuple),
                    cache: col.OrderedDict = None):
        """
        Uses the values in a cached row to fill in values in the current
        row by index. Useful when your dataset has grouped rows.

        Args:
            row: An OrderedDict.
            cols: A list of keys, which must be found in row.
            cache: An OrderedDict, which contains values to be
                pulled by key in cols into row. If cache is None,
                extrapolate will just return a copy of row.

        Returns: row with null values overwritten with populated
            values from the cached OrderedDict.

        """
        result = row.copy()
        if cache is not None:
            for c in cols:
                if result[c] is None:
                    result[c] = cache[c]
        return result

    @staticmethod
    @parser('collect_rejects', priority=9)
    def cleanse_incomplete_rows(
            row: col.OrderedDict, meta_data: e.MetaData) -> col.OrderedDict:
        """
        A not terribly useful parser that removes rows that are missing
        non-nullable columns.

        # TODO: Revamp this parser so that it can more intelligently
                remove incomplete rows. Possibly by having it
                specifically remove total rows.
        Args:
            row:
            meta_data:

        Returns:

        """
        result = row
        if meta_data.check_key('nullable'):
            required_cols = []
            for k, v in meta_data.items():
                if not v['nullable']:
                    required_cols.append(k)
            row_req = u.collect_by_keys(row, *required_cols)
            if u.count_nulls(row_req) > 0:
                result = None
        return result

    @staticmethod
    @parser
    def clean_typos(row: dict, meta_data: dict):
        typo_funcs = {
            'numeric': Clean.clean_numeric_typos
        }
        result = dict()
        for k, v in row.items():
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
            ParserSubset(
                self.uniques_report,
                self.types_report,
                self.nulls_report
            ),
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
    @parser(parses='column', requires_format='any')
    def nulls_report(column: list, col_name: str,
                     meta_data: e.MetaData) -> list:
        """
        Takes a list and creates a dictionary report on the null values
        found in the list and uses it to update meta_data.

        Args:
            column: A list.
            col_name: A string indicating the name of the column this
                data came from.
            meta_data: A MetaData object.

        Returns: column

        """
        null_ct = u.count_nulls(column)
        meta_data.update(
            col_name,
            null_ct=null_ct,
            nullable=True if null_ct > 0 else False
        )
        return column

    @staticmethod
    @parser(parses='column', requires_format='lists')
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
            col_name,
            str_pct=str_pct,
            num_pct=num_pct,
            probable_type=prob_type
        )
        return column

    @staticmethod
    @parser(parses='column', requires_format='lists')
    def uniques_report(column: list, col_name: str, meta_data: e.MetaData) -> list:
        """
        Takes a list and creates a dictionary report on the unique
        values of data found in the list and uses it to update
        meta_data.

        # TODO: Add functionality to not count nulls as uniques?

        Args:
            column: A list.
            col_name: A string indicating the name of the column this
                data came from.
            meta_data: A MetaData object.

        Returns: column

        """
        uniques = set(column)
        unique_ct = len(uniques)
        pk = len(uniques) == len(column)
        meta_data.update(
            col_name, unique_ct=unique_ct, primary_key=pk)
        return column
