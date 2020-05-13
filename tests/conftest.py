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
    return [
        ['Sales by Location Report', '', ''],
        ['Grouping: Region', '', ''],
        ['', '', ''],
        ['', '', ''],
        ['location', 'region', 'sales'],
        ['Bayside Store', 'Northern', 500],
        ['West Valley Store', 'Northern', 300],
        ['', '', 800],
        ['Precioso Store', 'Southern', 1000],
        ['Kalliope Store', 'Southern', 200],
        ['', '', 1200]
    ]
