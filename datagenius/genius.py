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
        for p in self.steps:
            wdset.data = wdset.loop(p)
        return wdset


class Preprocess(Genius):
    def __init__(self, *custom_steps):
        preprocess_steps = [
            pa.cleanse_gap,
            *custom_steps
        ]
        super(Preprocess, self).__init__(*preprocess_steps)

    def go(self, dset: ds.Dataset, **options):
        wdset = super(Preprocess, self).go(dset, **options)
        wdset.header = wdset.loop(
            options.get('header_func', pa.detect_header)
        )[0]
        return wdset



