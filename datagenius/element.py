import datagenius.util as u


class ZeroNumeric:
    # TODO: Make this more powerful by storing the zeros separately
    #       and the numeric portion as a numeric value, so that
    #       mathematical operations can be carried out on it without
    #       sacrificing the initial zeros.
    """
    Numeric strings that need to have one or more zeros on their left
    side.
    """
    def __init__(self, value):
        """

        Args:
            value: A string that could be converted to a numeric value.
        """
        if not u.isnumericplus(value):
            raise ValueError(f'ZeroNumeric must be a numeric string or '
                             f'a numeric value. Invalid value={value}')
        self._value = str(value)

    def pad(self, length: int) -> str:
        """
        Returns the value of the ZeroNumeric but with additional zeros
        on the left side to ensure the length of the ZeroNumeric is
        equal to length.

        Args:
            length: An integer, the desired character length of the
                padded ZeroNumeric

        Returns: The value of the ZeroNumeric with as many zeros on the
            left as are necessary for len(ZeroNumeric) == length.

        """
        return '0' * max(length - len(self), 0) + self._value

    def __eq__(self, other):
        return self._value == other

    def __len__(self):
        return len(self._value)

    def __ne__(self, other):
        return self._value != other

    def __repr__(self):
        return self._value

    def __str__(self):
        return "'" + self._value
