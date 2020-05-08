import datagenius.dataset as ds


class Genius:
    def __init__(self, dataset: ds.Dataset, **kwargs):
        hfunc = kwargs.get('header_func', self.header_func)

    @staticmethod
    def header_func(dataset: ds.Dataset) -> (int, list):
        """
        Takes a dataset (a list of lists) and determines which row
        is most likely to be the header row. This simple function
        assumes that the header row is the row that has a filled
        string value for every column in the dataset.

        Args:
            dataset: A datagenius Dataset object.

        Returns: An integer indicating the index of the header row
            of the passed Dataset.

        """
        width = dataset.col_ct
        header = []
        header_idx = None
        str_ctr = ds.Parser(
            lambda x: [
                1 if isinstance(y, str) and y != '' else 0 for y in x]
        )
        find_hdr = ds.Parser(
            lambda x: ((x, True) if sum(str_ctr(x)) == width
                       else (None, False))
        )
        h = dataset.loop(find_hdr)
        if len(h) > 0:
            header = h[0]
            header_idx = dataset.index(header)
        return header_idx, header
