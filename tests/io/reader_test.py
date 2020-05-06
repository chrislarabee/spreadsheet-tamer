from collections import OrderedDict

from datagenius.io import reader


def test_read_csv():
    expected = [
        OrderedDict(id='1', fname='Yancy', lname='Cordwainer'),
        OrderedDict(id='2', fname='Muhammad', lname='El-Kanan'),
        OrderedDict(id='3', fname='Luisa', lname='Romero'),
        OrderedDict(id='4', fname='Semaj', lname='Soto')
    ]

    assert reader.read_csv('tests/samples/simple_csv.csv') == expected
