import inspect


class Parser:
    """
    Stores a function that takes a single list argument and does
    something to or with the list argument. This is used to regularize
    all the inputs for the various datagenius objects, so that their
    ETL steps can be highly modular.
    """
    def __init__(self, func=None, **kwargs):
        """

        Args:
            func: A callable object that takes one argument.
            kwargs: Various ways to customize Parser's behaviors.
                Currently in use kwargs:
                null_ind: What func returns when it returns an
                    empty value. Default is None, but this parameter
                    can be overridden if None is a meaningful value
                    for your func.
                riders: A single object or tuple of additional
                    values Parser should append to the result of
                    func.

        """
        self.func = func
        self.null_ind = kwargs.get('null_ind', None)
        self.riders = kwargs.get('riders')
        if ((not isinstance(self.riders, tuple)
                or not isinstance(self.riders, list))
                and self.riders is not None):
            self.riders = tuple([self.riders])

    def __call__(self, data: list):
        """
        When the parser is called with a list argument, it applies
        its func function to the argument and returns the result.

        Args:
            data: A list.

        Returns: The results of self.func using the passed list.

        """
        if self.func is not None:
            if self.riders is not None:
                return (self.func(data), *self.riders)
            else:
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
    def __init__(self, data: list, **kwargs):
        """
        Datasets must be instantiated with a list of lists.

        Args:
            data: A list of lists.
            kwargs: Various ways to customize Dataset's behaviors.
                Currently in use kwargs:
                threshold: An integer, the number of
                    non-null/blank values that a row must have to
                    be included in the dataset. By default this will
                    be the number of columns in the dataset - 1
                    in order to automatically weed out obvious
                    subtotal rows.
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
                self.needs_preprocess = True
                # Attributes to allow iteration.
                self.cur_idx = -1
                self.max_idx = len(data)
            else:
                raise ValueError(struct_error_msg)
        else:
            raise ValueError(struct_error_msg)
        self.threshold = self.col_ct - kwargs.get('threshold', 1)

    def loop(self, p: Parser) -> list:
        """
        Loops over all the rows in self.data and passes each to p.

        Args:
            p: A Parser object that takes a list (a row
                of Dataset) and evaluates it somehow. It must
                return a tuple of two values, the second of which
                must be a boolean indicating whether to break the
                loop or not (True breaks).

        Returns: A list containing the results of p's evaluation
            of each row.

        """
        result = []
        for i in self:
            r, break_ind = p(i)
            if r != p.null_ind:
                result.append(r)
            if break_ind:
                break
        return result

    def preprocess(self, threshold: int = None):
        """
        Uses loop to reduce data to only rows that contain
        sufficient non-null values.

        Args:
            threshold: Updates Dataset's existing threshold
                attribute directly before preprocessing.

        Returns: None

        """
        if self.needs_preprocess:
            non_null_ctr = Parser(
                lambda x: [0 if y in (None, '') else 1 for y in x]
            )
            if threshold is not None:
                self.threshold = threshold
            cleaner = Parser(
                lambda x: (
                    None if sum(non_null_ctr(x)) <= self.threshold
                    else x),
                riders=False
            )
            self.data = self.loop(cleaner)
            self.needs_preprocess = False

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
