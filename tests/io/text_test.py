import os
import warnings

import pytest

from datagenius.io import text


created_ids = []


class TestSheetsAPI:
    no_creds_msg = 'No credentials set up for google api.'

    def test_create_and_delete_folder(self, sheets_api):
        if not sheets_api:
            pytest.skip(self.no_creds_msg)
        global created_ids
        folder = 'data_genius_test_folder'
        f_id = sheets_api.create_folder(folder)
        created_ids.append(f_id)
        f = sheets_api.find_objects(folder, 'folder')
        assert len(f) > 0
        assert f[0].get('name') == folder


def test_build_template(customers):
    t = text.get_output_template('tests/samples/csv/simple.csv')
    assert t == customers()['columns']

