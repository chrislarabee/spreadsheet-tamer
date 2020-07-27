from datetime import datetime as dt

import pandas as pd

from datagenius.io import text
from tests import testing_tools


def test_write_gsheet_and_from_gsheet(sheets_api):
    testing_tools.check_sheets_api_skip(sheets_api)

    sheet = f'data_genius_test_sheet {dt.now()}'
    df = pd.DataFrame([dict(a=1, b=2), dict(a=3, b=4)])
    sheet_id, shape = text.write_gsheet(sheet, df, sheets_api)
    testing_tools.created_ids.append(sheet_id)
    expected = pd.DataFrame([
        ['a', 'b'],
        ['1', '2'],
        ['3', '4']
    ])
    assert shape == (3, 2)
    read_df = text.from_gsheet(sheet + '.sheet', sheets_api)
    print(read_df.columns)
    pd.testing.assert_frame_equal(read_df, expected)


class TestSheetsAPI:
    def test_create_object(self, sheets_api):
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

        # Create a file IN the folder:
        sheet = f'data_genius_test_sheet_in_folder {dt.now()}'
        sf_id = sheets_api.create_object(sheet, 'sheet', f_id)
        testing_tools.created_ids.insert(0, sf_id)
        f = sheets_api.find_object(sheet, 'sheet')
        assert len(f) > 0
        assert f[0].get('name') == sheet
        assert f[0].get('parents')[0] == f_id


def test_build_template(customers):
    t = text.get_output_template('tests/samples/csv/simple.csv')
    assert t == customers()['columns']

