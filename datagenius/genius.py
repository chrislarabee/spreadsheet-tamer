import datagenius.dataset as ds
import datagenius.parsers as pa
import datagenius.util as u


class Genius:
    def __init__(self, *steps):
        self.steps = []
        for s in steps:
            if u.validate_parser(s):
                self.steps.append(s)
            else:
                raise ValueError(f'Genius objects only take parser'
                                 f'functions as steps. Invalid '
                                 f'function={s.__name__}')

    def go(self, dset: ds.Dataset, **options):
        if options.get('overwrite', True):
            wdset = dset
        else:
            wdset = dset.copy()
        wdset.data = self.loop(wdset, *self.steps)
        return wdset

    @staticmethod
    def loop(dset: ds.Dataset, *parsers) -> list:
        """
        Loops over all the rows in the passed Dataset and passes
        each to the passed parsers.

        Args:
            dset: A Dataset object.
            parsers: One or more parser functions.

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
        return results


class Preprocess(Genius):
    def __init__(self, *custom_steps):
        preprocess_steps = [
            pa.cleanse_gap,
            *custom_steps
        ]
        super(Preprocess, self).__init__(*preprocess_steps)

    def go(self, dset: ds.Dataset, **options):
        wdset = super(Preprocess, self).go(dset, **options)
        wdset.header = self.loop(
            wdset,
            options.get('header_func', pa.detect_header)
        )[0]
        return wdset



