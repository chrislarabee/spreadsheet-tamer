import pytest


@pytest.fixture
def simple_data():
    return [
            ['id', 'fname', 'lname'],
            [1, 'Yancy', 'Cordwainer'],
            [2, 'Muhammad', 'El-Kanan'],
            [3, 'Luisa', 'Romero'],
            [4, 'Semaj', 'Soto']
        ]


@pytest.fixture
def gaps():
    return [
            ['', '', ''],
            ['', '', ''],
            ['', '', ''],
            ['', '', ''],
            ['id', 'fname', 'lname'],
            ['', '', ''],
            [1, 'Yancy', 'Cordwainer'],
            [2, 'Muhammad', 'El-Kanan'],
            [3, 'Luisa', 'Romero'],
            [4, 'Semaj', 'Soto']
        ]


@pytest.fixture
def gaps_totals():
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
