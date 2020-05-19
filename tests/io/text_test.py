import xlrd

from datagenius.io import text


def test_read_csv(simple_data, gaps):
    expected = {
        'simple': simple_data()
    }

    assert text.read_csv('tests/samples/csv/simple.csv') == expected

    # Test ability to handle badly formatted csvs:
    expected = {
        'gaps': gaps
    }

    assert text.read_csv('tests/samples/csv/gaps.csv') == expected


def test_read_sheet(simple_data, gaps_totals):
    with xlrd.open_workbook(
            'tests/samples/excel/simple.xlsx') as wb:
        assert text.read_sheet(wb.sheet_by_index(0)) == simple_data(int)

    with xlrd.open_workbook(
            'tests/samples/excel/gaps_totals.xlsx') as wb:
        assert text.read_sheet(wb.sheet_by_index(0)) == gaps_totals
