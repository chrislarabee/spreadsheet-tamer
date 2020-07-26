from datetime import datetime as dt

import pytest

from datagenius.io import text


created_ids = []


class TestSheetsAPI:
    no_creds_msg = 'No credentials set up for google api.'

    def test_create_object(self, sheets_api):
        if not sheets_api:
            pytest.skip(self.no_creds_msg)
        global created_ids

        # Create a folder:
        folder = f'data_genius_test_folder {dt.now()}'
        f_id = sheets_api.create_object(folder, 'folder')
        created_ids.append(f_id)
        f = sheets_api.find_objects(folder, 'folder')
        assert len(f) > 0
        assert f[0].get('name') == folder

        # Create a file:
        sheet = f'data_genius_test_sheet {dt.now()}'
        s_id = sheets_api.create_object(sheet, 'sheet')
        created_ids.append(s_id)
        f = sheets_api.find_objects(sheet, 'sheet')
        assert len(f) > 0
        assert f[0].get('name') == sheet

        # Create a file IN the folder:
        sheet = f'data_genius_test_sheet_in_folder {dt.now()}'
        sf_id = sheets_api.create_object(sheet, 'sheet', f_id)
        created_ids.append(sf_id)
        f = sheets_api.find_objects(sheet, 'sheet')
        assert len(f) > 0
        assert f[0].get('name') == sheet
        assert f[0].get('parents')[0] == f_id


def test_build_template(customers):
    t = text.get_output_template('tests/samples/csv/simple.csv')
    assert t == customers()['columns']

