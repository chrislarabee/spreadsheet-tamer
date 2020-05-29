import pytest


@pytest.fixture
def customers():
    return (
        ['id', 'fname', 'lname', 'foreign_key'],
        [
            ['1', 'Yancy', 'Cordwainer', '00025'],
            ['2', 'Muhammad', 'El-Kanan', '00076'],
            ['3', 'Luisa', 'Romero', '00123'],
            ['4', 'Semaj', 'Soto', '01234']
        ]
    )


@pytest.fixture
def sales():
    return (
        ['location', 'region', 'sales'],
        [
            ['Bayside Store', 'Northern', 500],
            ['West Valley Store', 'Northern', 300],
            ['Precioso Store', 'Southern', 1000],
            ['Kalliope Store', 'Southern', 200],
        ]
    )


@pytest.fixture
def products():
    return (
        ['id', 'name', 'price', 'cost', 'upc', 'attr1', 'attr2', 'attr3',
         'attr4', 'attr5'],
        [
            [1, 'Widget', 8.5, 4.0, 1234567890, None, None, None, None, None],
            [2, 'Doohickey', 9.99, 5.0, 2345678901, 'copper', 'large', None, None, None],
            [3, 'Flange', 1.0, 0.2, 3456789012, 'steel', 'small', None, None, None]
        ]
    )


@pytest.fixture
def simple_data():
    """
    Generates a simple, ideal dataset for tests. The inner function
    _gen is used so that simple_data() can return its data with
    the id values interpreted as strings (for csv reading) or
    integers (for excel reading).

    Returns: A list of lists.

    """
    def _gen(f=str):
        d = [
            ['id', 'fname', 'lname', 'foreign_key'],
            [f(1), 'Yancy', 'Cordwainer', '00025'],
            [f(2), 'Muhammad', 'El-Kanan', '00076'],
            [f(3), 'Luisa', 'Romero', '00123'],
            [f(4), 'Semaj', 'Soto', '01234']
        ]

        return d

    return _gen


@pytest.fixture
def gaps():
    """
    Generates a dataset with gap rows from bad formatting for
    testing.

    Returns: A list of lists.

    """
    return [
            ['', '', '', ''],
            ['', '', '', ''],
            ['', '', '', ''],
            ['', '', '', ''],
            ['id', 'fname', 'lname', 'foreign_key'],
            ['', '', '', ''],
            ['1', 'Yancy', 'Cordwainer', '00025'],
            ['2', 'Muhammad', 'El-Kanan', '00076'],
            ['3', 'Luisa', 'Romero', '00123'],
            ['4', 'Semaj', 'Soto', '01234']
        ]


@pytest.fixture
def gaps_totals():
    """
    Generates a 'report-like' dataset that has sub-total rows and
    some titles and sub-titles and such for testing.

    Returns: A list of lists.

    """
    def _gen(w_gaps=True, w_pre_header=True):
        ph = [
            ['Sales by Location Report', '', ''],
            ['Grouping: Region', '', '']
        ]
        g = [
            ['', '', ''],
            ['', '', '']
        ]
        x = [
            ['location', 'region', 'sales'],
            ['Bayside Store', 'Northern', 500],
            ['West Valley Store', 'Northern', 300],
            ['', '', 800],
            ['Precioso Store', 'Southern', 1000],
            ['Kalliope Store', 'Southern', 200],
            ['', '', 1200]
        ]
        y = []
        if w_pre_header:
            y = ph
        if w_gaps:
            y = [*y, *g]
        return [*y, *x]
    return _gen


@pytest.fixture
def needs_cleanse_totals():
    return (
        ['location', 'region', 'sales'],
        [
            ['Bayside Store', 'Northern', 500],
            ['West Valley Store', 'Northern', 300],
            [None, None, 800],
            ['Precioso Store', 'Southern', 1000],
            ['Kalliope Store', 'Southern', 200],
            [None, None, 1200]
        ]
    )


@pytest.fixture
def needs_extrapolation():
    return (
        ['product_id', 'vendor_name', 'product_name'],
        [
            [1, 'StrexCorp', 'Teeth'],
            [2, None, 'Radio Equipment'],
            [3, 'KVX Bank', 'Bribe'],
            [4, None, 'Not candy or pens']
        ]
    )


@pytest.fixture
def needs_rules():
    return (
        ['id', 'name', 'price', 'cost', 'upc', 'attr1', 'attr2', 'attr3',
         'attr4', 'attr5'],
        [
            [1, 'Widget', 8.5, 4.0, 1234567890, None, None, None, None, None],
            [2, 'Doohickey', 9.99, 5.0, 2345678901, 'cu', 'large', None, None, None],
            [3, 'Flange', 1.0, 0.2, 3456789012, 'steel', 'sm', None, None, None]
        ]
    )
