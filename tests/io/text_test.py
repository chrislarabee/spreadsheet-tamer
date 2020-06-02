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
        assert text.read_sheet(wb.sheet_by_index(0)) == gaps_totals()


def test_build_template(customers):
    t = text.build_template('tests/samples/csv/simple.csv')
    assert t == customers[0]

    t = text.build_template('tests/samples/excel/simple.xlsx')
    assert t == customers[0]


def test_write_csv():
    p = 'tests/samples/text_test.csv'
    d = [
        {'id': '0001', 'name': 'Herman'},
        {'id': '0002', 'name': 'Fatima'}
    ]
    text.write_csv(p, d, ['id', 'name'])
    expected = [
        ['id', 'name'],
        ['0001', 'Herman'],
        ['0002', 'Fatima']
    ]
    assert text.read_csv(p)['text_test'] == expected
