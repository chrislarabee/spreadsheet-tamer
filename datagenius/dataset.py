import inspect


class Parser:
    """
    Stores a function that takes a single list argument and does
    something to or with the list argument. This is used to regularize
    all the inputs for the various datagenius objects, so that their
    ETL steps can be highly modular.
    """
    def __init__(self, func=None):
        self.func = func

    def __call__(self, data: list):
        """
        When the parser is called with a list argument, it applies
        its func function to the argument and returns the result.

        Args:
            data: A list.

        Returns: The results of self.func using the passed list.

        """
        if self.func is not None:
            return self.func(data)
        else:
            raise ValueError('No parsing function defined.')

    def __setattr__(self, key: str, value):
        """
        Overrides inbuilt __setattr__ so that when self.func is set,
        it throws an error if the passed value doesn't correspond
        to Parser's rules for its func.

        Args:
            key: A string, the name of an existing or new attribute
                for the Parser object.
            value: The value of the existing or new attribute for the
                Parser object.

        Returns: None

        """
        if key == 'func' and value is not None:
            if callable(value):
                if len(inspect.signature(value).parameters) != 1:
                    raise ValueError('Parser only takes functions '
                                     'that have one argument for '
                                     'func.')
            else:
                raise ValueError('Parser only takes callable '
                                 'arguments for func.')
        self.__dict__[key] = value


class Dataset:
    """
    A wrapper object for lists of lists. Datasets are the primary
    data-containing object for datagenius.
    """
    def __init__(self, data: list):
        """
        Datasets must be instantiated with a list of lists.

        Args:
            data: A list of lists.
        """
        struct_error_msg = 'Dataset data must be a list of lists.'
        if isinstance(data, list):
            if isinstance(data[0], list):
                self.col_ct = len(data[0])
                for d in data:
                    if len(d) != self.col_ct:
                        raise ValueError(f'All rows must have the '
                                         f'same length. Invalid row= '
                                         f'{d}')
                self.data = data
                # Attributes to allow iteration.
                self.cur_idx = -1
                self.max_idx = len(data)
            else:
                raise ValueError(struct_error_msg)
        else:
            raise ValueError(struct_error_msg)

    def match(self, match_parser: Parser,
              break_on_match=False) -> list:
        """
        Loops over all the rows in self.data and returns the ones
        that meet match_parser's criteria.

        Args:
            match_parser: A Parser object that takes a list (a row
                of Dataset) and evaluates it somehow, returning
                True if it matches and False if it doesn't.
            break_on_match: A boolean, indicates whether match
                should stop looping after the first row that meets
                match_parser's criteria.

        Returns: A list containing the rows that meet match_parser's
            criteria.

        """
        result = []
        for i in self:
            if match_parser(i):
                result.append(i)
                if break_on_match:
                    break
        return result

    def preprocess(self, threshold: int = -1):
        """
        Uses match to reduce data to only rows that contain
        sufficient non-null values.

        Args:
            threshold: An integer, the number of null/blank values
                that disqualifies a row from inclusion in the
                dataset. By default this will be the number of
                columns in the dataset - 1. So at least 2 fields
                in the row must have data for it to survive
                preprocessing.

        Returns: None

        """
        null_ctr = Parser(
            lambda x: [1 if y in (None, '') else 0 for y in x]
        )
        if threshold == -1:
            t = self.col_ct - 1
        else:
            t = threshold
        cleaner = Parser(
            lambda x: False if sum(null_ctr(x)) >= t else True
        )
        self.data = self.match(cleaner)

    def __eq__(self, other) -> bool:
        """
        Overrides built-in object equality so that Datasets
        used in == statements compare the list in self.data
        rather than the Dataset object itself.

        Args:
            other: Any object.

        Returns: A boolean indicating whether the value of self.data
            is equivalent to other.

        """
        if self.data == other:
            return True
        else:
            return False

    def __iter__(self):
        """
        Makes Dataset iterable.

        Returns: self.

        """
        return self

    def __next__(self):
        """
        Makes Dataset an iterable.

        Returns: The next value in self.data, or StopIteration
            if the end of self.data has been reached.

        """
        self.cur_idx += 1
        if self.cur_idx < self.max_idx:
            return self.data[self.cur_idx]
        else:
            self.cur_idx = -1
            raise StopIteration

    def __ne__(self, other) -> bool:
        """
        Overrides built-in object inequality so that Dataset's
        used in != statements compare the list in self.data
        rather than the Dataset object itself.

        Args:
            other: Any object.

        Returns: A  boolean indicating whether the value of self.data
            is not equivalent to other.

        """
        if self.data != other:
            return True
        else:
            return False
