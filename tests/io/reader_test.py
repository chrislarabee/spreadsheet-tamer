from collections import OrderedDict

import xlrd

from datagenius.io import reader


def test_read_csv():
    expected = [
        ['id', 'fname', 'lname'],
        ['1', 'Yancy', 'Cordwainer'],
        ['2', 'Muhammad', 'El-Kanan'],
        ['3', 'Luisa', 'Romero'],
        ['4', 'Semaj', 'Soto']
    ]

    assert reader.read_csv('tests/samples/csv/simple.csv') == expected

    # Test ability to handle badly formatted csvs:
    expected = [
        ['', '', ''],
        ['', '', ''],
        ['', '', ''],
        ['', '', ''],
        ['id', 'fname', 'lname'],
        ['', '', ''],
        ['1', 'Yancy', 'Cordwainer'],
        ['2', 'Muhammad', 'El-Kanan'],
        ['3', 'Luisa', 'Romero'],
        ['4', 'Semaj', 'Soto']
    ]

    assert reader.read_csv('tests/samples/csv/gaps.csv') == expected


def test_read_sheet():
    wb = xlrd.open_workbook('tests/samples/excel/simple.xlsx')

    expected = [
        ['id', 'fname', 'lname'],
        [1, 'Yancy', 'Cordwainer'],
        [2, 'Muhammad', 'El-Kanan'],
        [3, 'Luisa', 'Romero'],
        [4, 'Semaj', 'Soto']
    ]

    assert reader.read_sheet(wb.sheet_by_index(0)) == expected

    wb = xlrd.open_workbook('tests/samples/excel/gaps_totals.xlsx')

    expected = [
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

    assert reader.read_sheet(wb.sheet_by_index(0)) == expected
