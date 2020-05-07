import xlrd

from datagenius.io import reader


def test_read_csv(simple_data, gaps):
    expected = {
        'simple': simple_data
    }

    assert reader.read_csv('tests/samples/csv/simple.csv') == expected

    # Test ability to handle badly formatted csvs:
    expected = {
        'gaps': gaps
    }

    assert reader.read_csv('tests/samples/csv/gaps.csv') == expected


def test_read_sheet(simple_data, gaps_totals):
    with xlrd.open_workbook(
            'tests/samples/excel/simple.xlsx') as wb:
        assert reader.read_sheet(wb.sheet_by_index(0)) == simple_data

    with xlrd.open_workbook(
            'tests/samples/excel/gaps_totals.xlsx') as wb:
        assert reader.read_sheet(wb.sheet_by_index(0)) == gaps_totals
