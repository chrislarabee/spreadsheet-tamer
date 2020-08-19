from datetime import datetime as dt

import pandas as pd

from datagenius.io import text
from tests import testing_tools


def test_write_gsheet_and_from_gsheet(sheets_api):
    testing_tools.check_sheets_api_skip(sheets_api)

    sheet = f'data_genius_test_sheet {dt.now()}'
    df = pd.DataFrame([dict(a=1, b=2), dict(a=3, b=4)])
    sheet_id, shape = text.write_gsheet(sheet, df, s_api=sheets_api)
    testing_tools.created_ids.append(sheet_id)
    expected = pd.DataFrame([
        ['a', 'b'],
        ['1', '2'],
        ['3', '4']
    ])
    assert shape == (3, 2)
    read_df = text.from_gsheet(sheet + '.sheet', sheets_api)
    pd.testing.assert_frame_equal(read_df, expected)

    # Write to a new sheet:
    df = pd.DataFrame([dict(c=5, d=6), dict(c=7, d=8)])
    sheet_id2, shape = text.write_gsheet(
        sheet, df, sheet_title='test_sheet', s_api=sheets_api)
    assert sheet_id2 == sheet_id
    expected = pd.DataFrame([
        ['c', 'd'],
        ['5', '6'],
        ['7', '8']
    ])
    assert shape == (3, 2)
    read_df = text.from_gsheet(sheet + '.sheet', sheets_api, 'test_sheet')
    pd.testing.assert_frame_equal(read_df, expected)


class TestSheetsAPI:
    def test_basics(self, sheets_api):
        testing_tools.check_sheets_api_skip(sheets_api)

        # Create a folder:
        folder = f'data_genius_test_folder {dt.now()}'
        f_id = sheets_api.create_object(folder, 'folder')
        testing_tools.created_ids.append(f_id)
        f = sheets_api.find_object(folder, 'folder')
        assert len(f) > 0
        assert f[0].get('name') == folder

        # Create a file:
        sheet = f'data_genius_test_sheet {dt.now()}'
        s_id = sheets_api.create_object(sheet, 'sheet')
        testing_tools.created_ids.insert(0, s_id)
        f = sheets_api.find_object(sheet, 'sheet')
        assert len(f) > 0
        assert f[0].get('name') == sheet

        # Add sheets to it:
        result = sheets_api.add_sheet(s_id)
        assert result == ('Sheet2', 1)

        result = sheets_api.add_sheet(s_id, title='test title')
        assert result == ('test title', 2)

        # Create a file IN the folder:
        sheet = f'data_genius_test_sheet_in_folder {dt.now()}'
        sf_id = sheets_api.create_object(sheet, 'sheet', f_id)
        testing_tools.created_ids.insert(0, sf_id)
        f = sheets_api.find_object(sheet, 'sheet')
        assert len(f) > 0
        assert f[0].get('name') == sheet
        assert f[0].get('parents')[0] == f_id


class TestGSheetFormatting:
    def test_basics(self):
        f = text.GSheetFormatting('fake_id')
        d = f.auto_dim_size
        d['autoResizeDimensions']['dimensions'] = dict(test=0)
        assert f.auto_dim_size == dict(
            autoResizeDimensions=dict(dimensions=None))

    def test_insert_rows(self):
        f = text.GSheetFormatting()
        f.set_file('fake_id').insert_rows(3, 2)
        assert f.requests == [
            dict(
                insertDimension=dict(
                    range=dict(
                        sheetId='fake_id',
                        dimension='ROWS',
                        startIndex=1,
                        endIndex=4
                    ),
                    inheritFromBefore=False
                )
            )
        ]


def test_build_template(customers):
    t = text.get_output_template('tests/samples/csv/customers.csv')
    assert t == customers()['columns']

