import datagenius.dataset as ds
import datagenius.parsers as pa
import datagenius.util as u


class Genius:
    """
    The base class for pre-built and custom genius objects.
    Provides methods and attributes to assist in creating
    transforms that are as smart and flexible as possible.
    """
    def __init__(self, *steps, parse_order: tuple = ('set', 'row')):
        """

        Args:
            *steps: Any number of parser functions.
            parse_order: A tuple of strings that match attributes
                found on the Genius object. Used to customize the
                order in which the different types of parser
                functions are executed by the Genius.
        """
        self.order = parse_order
        self.set = []
        self.row = []
        for s in steps:
            if u.validate_parser(s):
                if s.set_parser:
                    self.set.append(s)
                else:
                    self.row.append(s)
            else:
                raise ValueError(f'Genius objects only take parser'
                                 f'functions as steps. Invalid '
                                 f'function={s.__name__}')

    def go(self, dset: ds.Dataset, **options) -> ds.Dataset:
        """
        Runs the parser functions found on the Genius object
        in the order specified by self.order and then in the
        order they're placed in in each attribute.

        Args:
            dset: A Dataset object.
            **options: Keywords for customizing the functionality
                of go. Currently in use options:
                    overwrite: A boolean, tells go whether to
                        overwrite the contents of dset with the
                        results of the loops.

        Returns: The Dataset or a copy of it.

        """
        if options.get('overwrite', True):
            wdset = dset
        else:
            wdset = dset.copy()
        for step in self.order:
            wdset.data = self.loop(wdset, *self.__dict__[step])
        return wdset

    @staticmethod
    def loop(dset: ds.Dataset, *parsers,
             one_return: bool = False) -> (list or None):
        """
        Loops over all the rows in the passed Dataset and passes
        each to the passed parsers.

        Args:
            dset: A Dataset object.
            parsers: One or more parser functions.
            one_return: A boolean, tells loop that the
                parsers will only result in a single
                object to return, so no need to wrap it
                in an outer list.

        Returns: A list containing the results of the parsers'
            evaluation of each row in dset.

        """
        results = []
        for i in dset:
            row = i.copy()
            passes_all = True
            # Used to break the outer loop too if a breaks_loop
            # parser evaluates successfully:
            outer_break = False
            for p in parsers:
                if not u.validate_parser(p):
                    raise ValueError('Genius.loop can only take '
                                     'functions decorated as '
                                     'parsers.')
                elif p.requires_header and dset.header is None:
                    raise ValueError('Passed parser requires a '
                                     'header, which this Dataset '
                                     'does not have yet.')
                else:
                    parse_result = p(row)
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
        if one_return:
            if len(results) > 0:
                return results[0]
            else:
                return None
        else:
            return results


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
            pa.cleanse_gap,
            *custom_steps
        ]
        super(Preprocess, self).__init__(*preprocess_steps)

    def go(self, dset: ds.Dataset, **options) -> ds.Dataset:
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
            wdset.header = self.loop(
                wdset,
                options.get('header_func', pa.detect_header),
                one_return=True
            )
            if wdset.header is not None:
                wdset.remove(wdset.header)
        return wdset



