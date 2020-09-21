import pickle
import os
import warnings
import re
from typing import List, Dict, Tuple

import pandas as pd
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request

from datagenius import util as u


class SheetsAPI:
    """
    Uses locally stored credentials to connect to Google Drive and
    Google Sheets.
    """
    @property
    def drive(self):
        return self._drive

    @property
    def google_obj_types(self) -> dict:
        """
        Object types used by the Google API to refer to various kinds
        of Drive objects.

        Returns: A dictionary of shorthand terms and full Google API
            terms.

        """
        return {
            'folder': 'application/vnd.google-apps.folder',
            'sheet': 'application/vnd.google-apps.spreadsheet',
        }

    @property
    def sheets(self):
        return self._sheets

    def __init__(self):
        self._drive = self._connect_drive()
        self._sheets = self._connect_sheets()

    def create_object(
            self,
            obj_name: str,
            obj_type: str,
            parent_id: str = None):
        """
        Creates a file or folder via the Google Drive connection.

        Args:
            obj_name: A string, the desired name of the object to
                create.
            obj_type: A string, the type of object to create. Must be
                one of the keys in SheetsAPI.google_obj_types.
            parent_id: A string, the id of the folder or Shared Drive
                to create the object in.

        Returns:

        """
        kwargs = dict(parents=[parent_id]) if parent_id else dict()
        file_metadata = dict(
            name=obj_name,
            mimeType=self.google_obj_types[obj_type],
            **kwargs
        )
        file = self.drive.files().create(
            body=file_metadata,
            fields='id',
            supportsAllDrives=True
        ).execute()
        return file.get('id')

    def delete_object(self, object_id: str) -> None:
        """
        Deletes the passed Google Object ID from the connected Google
        Drive.

        Args:
            object_id: A Google Object ID.

        Returns: None

        """
        self.drive.files().delete(
            fileId=object_id,
            supportsAllDrives=True
        ).execute()

    def find_object(
            self,
            obj_name: str,
            obj_type: str = None,
            drive_id: str = None) -> list:
        """
        Searches for a Google Drive Object in the attached Google Drive
        by name.

        Args:
            obj_name: A string, the name of the object, or part of it.
            obj_type: The type of object to restrict the search to.
                Must be one of the keys in SheetsAPI.google_obj_types.
            drive_id: The id of the Shared Drive to search within.

        Returns: A list of the matching Drive Object names and ids.

        """
        query = f"name = '{obj_name}'"
        if obj_type:
            query += f" and mimeType='{self.google_obj_types[obj_type]}'"
        kwargs = self._setup_drive_id_kwargs(drive_id)

        page_token = None
        results = []
        while True:
            response = self.drive.files().list(
                q=query,
                spaces='drive',
                fields='nextPageToken, files(id, name, parents)',
                pageToken=page_token,
                **kwargs
            ).execute()
            for file in response.get('files', []):
                results.append(dict(
                    name=file.get('name'),
                    id=file.get('id'),
                    parents=file.get('parents')
                    )
                )
            page_token = response.get('nextPageToken', None)
            if page_token is None:
                break
        return results

    def create_find(
            self,
            obj_name: str,
            obj_type: str,
            parent_folder: str = None,
            drive_id: str = None) -> Tuple[str, bool]:
        """
        Convenience method for checking if an object exists and creating
        it if it does not.

        Args:
            obj_name: The name of the object.
            obj_type: The type of the object. Must be one of the keys in
                SheetsAPI.google_obj_types.
            parent_folder: The name of the folder to save the new object
                to. Separate nested folders with /, as if it were a
                local file path.
            drive_id: The id of the Shared Drive to search for the folder
                path and to save to.

        Returns: A tuple containing the id of the object, and a boolean
            indicating whether the object is new or not.

        """
        p_folder_id = None
        if parent_folder:
            search_res = self.find_object(
                parent_folder,
                'folder',
                drive_id
            )
            if len(search_res) == 1:
                p_folder_id = search_res[0].get('id')
            else:
                warnings.warn(
                    f'Cannot find single exact match for {parent_folder}. '
                    f'Saving {obj_name} to root Drive.'
                )
        search_res = self.find_object(
            obj_name,
            obj_type,
            drive_id
        )
        new_obj = False
        if len(search_res) > 1:
            for result in search_res:
                if result.get('parents')[0] == p_folder_id:
                    file_id = result.get('id')
                    break
            else:
                raise ValueError(
                    f'Cannot find {obj_name} in {parent_folder}')
        elif len(search_res) == 1:
            file_id = search_res[0].get('id')
        else:
            new_obj = True
            file_id = self.create_object(obj_name, obj_type, p_folder_id)
        return file_id, new_obj

    def add_sheet(self, sheet_id: str, **sheet_properties):
        """
        Adds a new sheet to the Google Sheet at the passed id.

        Args:
            sheet_id: The id of a Google Sheet.
            **sheet_properties: The desired properties of the new
                sheet, such as:
                    title: The title of the sheet.

        Returns: The title and index position of the new sheet.

        """
        result = self.sheets.spreadsheets().batchUpdate(
            spreadsheetId=sheet_id,
            body={'requests': [{
                'addSheet': {
                    'properties': sheet_properties
                }
            }]}
        ).execute()
        r = result.get('replies')
        if r:
            r = r[0]['addSheet']['properties']
            return r.get('title'), r.get('index')
        else:
            return None

    def get_sheets(self, sheet_id: str) -> List[Dict[str, dict]]:
        """
        Gets a list of sheets within the Google Sheet located at the
        passed sheet_id.
        Args:
            sheet_id: A Google Sheet ID.

        Returns: A list of dictionaries, each being the properties of a
            sheet in the Google Sheet.

        """
        return self.sheets.spreadsheets().get(
            spreadsheetId=sheet_id,
            fields=(
                'sheets(data/rowData/values/userEnteredValue,'
                'properties(index,sheetId,title))')
        ).execute().get('sheets', [])

    def check_sheet_titles(
            self,
            sheet_title: str,
            sheet_id: str = None,
            sheets: list = None) -> (int, None):
        """
        Checks the sheets of the Google Sheet at the passed sheet_id,
        or just the passed sheets, for a sheet with a title matching
        that of sheet_title.

        Args:
            sheet_title: A string, the title of a sheet to search for.
            sheet_id: A string, the id of a Google Sheet.
            sheets: A list of sheet property dictionaries. Can be used
                in place of sheet_id if another process has already
                generated this list.

        Returns: An integer, the index of the sheet if it is found in
            the sheets of the Google Sheet, or None, if it is not.

        """
        if sheets is None and sheet_id is None:
            raise ValueError(
                'Must pass sheet_id or sheets to check_sheet_titles.')
        sheets = self.get_sheets(sheet_id) if not sheets else sheets
        idx = None
        for s in sheets:
            if s['properties']['title'] == sheet_title:
                idx = s['properties']['index']
        return idx

    def get_sheet_metadata(
            self,
            sheet_id: str,
            sheet_title: str = None) -> (dict, None):
        """
        Retrieves metadata about the first sheet in the Google Sheet
        corresponding to the passed sheet_id.

        Args:
            sheet_id: A string, the id of a Google Sheet.
            sheet_title: A string, the name of the sheet within the
                Google Sheet to get metadata for. If none, metadata for
                the first sheet will be used.

        Returns: A dictionary containing information about the data in
            the passed sheet, or None if the passed sheet does not
            exist.

        """
        raw = self.get_sheets(sheet_id)
        s_idx = 0
        if sheet_title is not None:
            s_idx = self.check_sheet_titles(sheet_title, sheets=raw)
        if s_idx is not None:
            sheet = raw[s_idx]
            # Newly created Google Sheets have no rowData.
            row_data = sheet['data'][0].get('rowData')
            if row_data:
                last_row_idx = len(row_data)
                last_col_idx = max([len(e['values']) for e in row_data if e])
            else:
                last_row_idx = 0
                last_col_idx = 0
            return dict(
                id=sheet['properties']['sheetId'],
                index=s_idx,
                title=sheet['properties']['title'],
                row_limit=last_row_idx,
                col_limit=last_col_idx,
            )

    def write_values(
            self,
            file_id: str,
            data: list,
            sheet_title: str = '',
            start_cell: str = 'A1') -> tuple:
        """
        Writes the passed data to the passed Google Sheet.

        Args:
            file_id: The file id of the Google Sheet.
            data: A list of lists, the data to write.
            sheet_title: The title of the sheet within the Google Sheet
                to write to. Default is the first sheet.
            start_cell: The starting cell to write to. Values will be
                written from left to write and top to bottom from
                this cell, overwriting any existing values.

        Returns: A tuple containing the # of rows and columns updated.

        """
        if sheet_title is not None:
            r = sheet_title + '!'
            s = self.check_sheet_titles(sheet_title, sheet_id=file_id)
            if s is None:
                self.add_sheet(file_id, title=sheet_title)
        else:
            r = ''
        result = self.sheets.spreadsheets().values().update(
            spreadsheetId=file_id,
            range=r + start_cell,
            valueInputOption='USER_ENTERED',
            body=dict(values=data)
        ).execute()
        return result.get('updatedRows'), result.get('updatedColumns')

    def get_sheet_values(self, file_id: str, sheet_title: str = None):
        """
        Gets all values from the passed Google Sheet id.

        Args:
            file_id: The id of the Google Sheet.
            sheet_title: The title of the desired sheet within the
                Google Sheet to pull values from. Default is the first
                sheet.

        Returns: A list of lists, the values from the sheet.

        """
        sheet_md = self.get_sheet_metadata(file_id, sheet_title)
        r = sheet_title + '!' if sheet_title else ''
        last_col_alpha = u.gen_alpha_keys(sheet_md['col_limit'])
        col_letter = last_col_alpha[sheet_md['col_limit'] - 1]
        result = self.sheets.spreadsheets().values().get(
            spreadsheetId=file_id,
            range=f"{r}A1:{col_letter}{sheet_md['row_limit']}"
        ).execute()
        return result.get('values', [])

    def batch_update(self, file_id: str, requests: list):
        """
        Executes a list of requests on the passed spreadsheet file.
        Args:
            file_id: The id of the Google Sheet.
            requests: A list of request dictionaries.

        Returns: The results of the batchUpdate.

        """
        return self._sheets.spreadsheets().batchUpdate(
            spreadsheetId=file_id,
            body=dict(requests=requests)
        ).execute()

    def format_sheet(self, file_id: str, sheet_title: str = None):
        """
        Instantiates a GSheetFormatting object for the passed Google
        Sheet and sheet title, which provides a variety of methods
        for specifying all the formatting changes you want.

        Args:
            file_id: The id of the Google Sheet.
            sheet_title: The name of a sheet in the Google Sheet,
                defaults to the first sheet.

        Returns: A GSheetFormatting object, which can be chained into
            its formatting methods and ended with a call to .execute()
            to apply the formatting, or used for other purposes.

        """
        sheet_id = self.get_sheet_metadata(file_id, sheet_title)['id']
        return GSheetFormatting(file_id, sheet_id, self)

    @staticmethod
    def _authenticate(scopes: list):
        """
        Uses locally stored credentials to attempt to login to Google
        Drive. The first time it is run it will cause a web page to
        open up and solicit permission to access the Google Drive as
        specified in the credentials. Then it will create a token that
        it will use going forward.

        Args:
            scopes: A list of scopes dictating the limits of the
                authenticated connection.

        Returns: The prepped credentials object.

        """
        creds = None

        # The file token.pickle stores the user's access and refresh
        # tokens, and is created automatically when the authorization
        # flow completes for the first time.
        if os.path.exists('token.pickle'):
            with open('token.pickle', 'rb') as token:
                creds = pickle.load(token)
        # If there are no (valid) credentials available, let the user
        # log in.
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file(
                    'credentials.json', scopes)
                creds = flow.run_local_server(port=0)
            # Save the credentials for the next run
            with open('token.pickle', 'wb') as token:
                pickle.dump(creds, token)

        return creds

    @staticmethod
    def _setup_drive_id_kwargs(drive_id: str = None) -> dict:
        """
        Whenever a drive_id is needed to access a shared drive, two
        other kwargs need to be passed to the relevant function. This
        method preps all three kwargs.

        Args:
            drive_id: The id of the shared drive to set up access to.

        Returns: A dictionary, either empty or containing the
            appropriate kwargs if drive_id is passed.

        """
        kwargs = dict()
        if drive_id:
            kwargs['corpora'] = 'drive'
            kwargs['driveId'] = drive_id
            kwargs['includeItemsFromAllDrives'] = True
            kwargs['supportsAllDrives'] = True
        return kwargs

    @classmethod
    def _connect_drive(cls):
        """
        Connects to the Google Drive specified in locally stored
        credentials.

        Returns: A connection to Google Drive.

        """
        scopes = [
            'https://www.googleapis.com/auth/drive'
        ]
        creds = cls._authenticate(scopes)

        return build('drive', 'v3', credentials=creds)

    @classmethod
    def _connect_sheets(cls):
        """
        Connects to the Google Sheets specified in locally stored
        credentials.

        Returns: A connection to Google Sheets.

        """
        scopes = [
            'https://www.googleapis.com/auth/spreadsheets'
        ]
        creds = cls._authenticate(scopes)

        return build('sheets', 'v4', credentials=creds)


class GSheetFormatting:
    number_fmt = ''
    accounting_fmt = (
        'NUMBER',
        '_($* #,##0.00_);_($* (#,##0.00);_($* "-"??_);_(@_)'
    )

    def __init__(
            self,
            file_id: str,
            sheet_id: int = 0,
            parent: SheetsAPI = None):
        """
        This object contains methods for creating a list of formatting
        requests and row/column operation requests that can then be
        processed with the execute() method.

        Args:
            file_id: The id of the Google Sheet to apply formatting to.
            sheet_id: The id of the sheet within the Google Sheet to
                apply formatting to.
            parent: The SheetsAPI object that generated this formatting.
        """
        self.parent: SheetsAPI = parent
        self.file_id: str = file_id
        self.sheet_id: int = sheet_id
        self.requests: list = []

    def execute(self) -> None:
        """
        Executes the amassed requests on this GSheetFormatting object.

        Returns: None

        """
        if self.parent:
            self.parent.batch_update(self.file_id, self.requests)

    def auto_column_width(self, start_col: int, end_col: int):
        """
        Adds an autoResizeDimensions request to the GSheetFormatting
            object's requests queue.

        Args:
            start_col: The 0-initial index of the first column to auto-
                resize.
            end_col: The 0-initial index of the last column to auto-
                resize.

        Returns: self

        """
        request = dict(
            autoResizeDimensions=dict(
                dimensions=self._build_dims_dict(
                    self.sheet_id, 'COLUMNS', start_col, end_col
                )
            )
        )
        self.requests.append(request)
        return self

    def append_rows(self, num_rows: int):
        """
        Adds the specified number of rows to the end of a Google Sheet.

        Args:
            num_rows: The number of rows to add.

        Returns: self

        """
        request = dict(
            appendDimension=dict(
                sheetId=self.sheet_id,
                dimension='ROWS',
                length=num_rows
            )
        )
        self.requests.append(request)
        return self

    def insert_rows(self, num_rows: int, at_row: int = 0):
        """
        Adds an insertDimension request to add more rows to the
        GSheetFormatting object's requests queue.

        WARNING: Do NOT use this method to add rows to the end of a
        Google Sheet. It will not work. Use append_rows instead.

        Args:
            num_rows: The # of rows to insert.
            at_row: The 0-initial index of the row to start inserting
               at.

        Returns: self.

        """
        request = self._insert_dims(
            self.sheet_id, 'ROWS', at_row, at_row + num_rows)
        self.requests.append(request)
        return self

    def delete_rows(self, start_row: int, end_row: int):
        """
        Adds a deleteDimension request to the GSheetFormatting object's
        requests queue.

        Args:
            start_row: The 0-initial index of the first row to delete.
            end_row: The 0-initial index of the last row to delete.

        Returns: self.

        """
        request = self._delete_dims(
            self.sheet_id, 'ROWS', start_row, end_row)
        self.requests.append(request)
        return self

    def _delete_dims(self, *vals) -> dict:
        return dict(
            deleteDimension=dict(
                range=self._build_dims_dict(*vals)
            )
        )

    def _insert_dims(self, *vals, inherit: bool = False) -> dict:
        """
        Creates an insertDimensions request.

        Args:
            *vals:  Values to be passed to _build_dims_dict.
            inherit: Indicates whether the inserted rows should inherit
                formatting from the rows before them.

        Returns: A dictionary request to insert new dimensions into a
            Google Sheet.

        """
        return dict(
            insertDimension=dict(
                range=self._build_dims_dict(*vals),
                inheritFromBefore=inherit
            )
        )

    @staticmethod
    def _build_dims_dict(*vals) -> dict:
        """
        Quick method for building a range/dimensions dictionary for use
        in a request dictionary wrapper intended to change Sheet
        dimensions (like inserting/deleting rows/columns or changing
        row/column widths).

        Args:
            *vals: One to 4 values, which will be slotted into the dict
                below in the order passed.

        Returns: A dictionary usable in a Google Sheets API request
            dictionary as either the range or dimensions value.

        """
        d = dict(
            sheetId=None,
            dimension=None,
            startIndex=None,
            endIndex=None
        )
        return dict(zip(d.keys(), vals))

    def apply_font(
            self,
            row_idxs: tuple = (None, None),
            col_idxs: tuple = (None, None),
            size: int = None,
            style: (str, tuple) = None):
        """
        Adds a textFormat request to the GSheetFormatting object's
        request queue.

        Args:
            row_idxs: A tuple of the start and end rows to apply font
                formatting to.
            col_idxs: A tuple of the start and end columns to apply font
                formatting to.
            size: Font size formatting.
            style: Font style formatting (bold, italic?, underline?).
                Bold is the only current style tested.

        Returns: self.

        """
        text_format = dict()
        if size:
            text_format['fontSize'] = size
        if style:
            style = u.tuplify(style)
            for s in style:
                text_format[s] = True
        repeat_cell = self._build_repeat_cell_dict(
            dict(textFormat=text_format),
            row_idxs,
            col_idxs,
            self.sheet_id
        )
        repeat_cell['fields'] = 'userEnteredFormat(textFormat)'
        request = dict(repeatCell=repeat_cell)
        self.requests.append(request)
        return self

    def apply_nbr_format(
            self,
            fmt: str,
            row_idxs: tuple = (None, None),
            col_idxs: tuple = (None, None)):
        """
        Adds a numberFormat request to the GSheetFormatting object's
        request queue.

        Args:
            fmt: A _fmt property from this object (like
                accounting_fmt) with or without the _fmt suffix.
            row_idxs: A tuple of the start and end rows to apply number
                formatting to.
            col_idxs: A tuple of the start and end columns to apply
                number formatting to.

        Returns: self.

        """
        fmt += '_fmt' if fmt[-4:] != '_fmt' else ''
        t, p = getattr(self, fmt)
        nbr_format = dict(type=t, pattern=p)

        repeat_cell = self._build_repeat_cell_dict(
            dict(numberFormat=nbr_format),
            row_idxs,
            col_idxs,
            self.sheet_id
        )
        repeat_cell['fields'] = 'userEnteredFormat.numberFormat'
        request = dict(repeatCell=repeat_cell)
        self.requests.append(request)
        return self

    def freeze(self, rows: int = None, columns: int = None):
        """
        Adds a freeze rows and/or columns request to the
        GSheetFormatting object's request queue.

        Args:
            rows: Number of rows to freeze.
            columns: Number of columns to freeze.

        Returns: self.

        """
        grid_prop = dict()
        if not rows and not columns:
            raise ValueError('One of rows or columns must not be None.')
        if rows:
            grid_prop['frozenRowCount'] = rows
        if columns:
            grid_prop['frozenColumnCount'] = columns
        request = dict(
            updateSheetProperties=dict(
                properties=dict(
                    sheetId=self.sheet_id,
                    gridProperties=grid_prop
                )
            )
        )
        self.requests.append(request)
        return self

    def alternate_row_background(
            self,
            row_idxs: tuple = (None, None),
            col_idxs: tuple = (None, None),
            *rgb_vals: float):
        """
        Adds a background of the specified color to every other row in
        the passed range.

        Args:
            row_idxs: A tuple of the start and stop row indexes.
            col_idxs: A tuple of the start and stop column indexes.
            *rgb_vals: Red, green, and blue values, in order. More than
                3 values will be ignored, default value is 0, so you
                only need to specify up to the last non-zero value.

        Returns: self.

        """
        request = dict(
            addConditionalFormatRule=dict(
                rule=dict(
                    ranges=[
                        self._build_range_dict(
                            self.sheet_id, row_idxs, col_idxs)
                    ],
                    booleanRule=dict(
                        condition=dict(
                            type='CUSTOM_FORMULA',
                            values=[
                                dict(userEneteredValue="=MOD(ROW(), 2)")
                            ]
                        ),
                        format=dict(
                            backgroundColor=self._build_color_dict(*rgb_vals)
                        )
                    )
                )
            )
        )
        self.requests.append(request)
        return self

    @classmethod
    def _build_repeat_cell_dict(
            cls,
            fmt_dict: dict,
            row_idxs: tuple = (None, None),
            col_idxs: tuple = (None, None),
            sheet_id: int = 0,) -> dict:
        """
        Quick method for building a repeatCell dictionary for use in a
        request dictionary wrapper intended to change cell formatting or
        contents (like changing font, borders, background, contents,
        etc).

        Args:
            fmt_dict: A formatting dictionary.
            row_idxs: A tuple of the start and stop row indexes.
            col_idxs: A tuple of the start and stop column indexes.
            sheet_id: The id of the sheet to apply the formatting to.
                Default is 0.

        Returns: A dictionary ready to be slotted in at the repeatCell
            key in a request.

        """
        return dict(
            range=cls._build_range_dict(sheet_id, row_idxs, col_idxs),
            cell=dict(
                userEnteredFormat=fmt_dict
            )
        )

    @staticmethod
    def _build_range_dict(
            sheet_id: int = 0,
            row_idxs: tuple = (None, None),
            col_idxs: tuple = (None, None)) -> dict:
        """
        Quick method for building a range dictionary for use in a
        request dictionary wrapper intended to change cell formatting or
        contents (like changing font, borders, background, contents,
        etc).

        Args:
            sheet_id: The id of the sheet to build a range for,
                default is 0, the first sheet.
            row_idxs: A tuple of the start and stop row indexes.
            col_idxs: A tuple of the start and stop column indexes.

        Returns: A dictionary ready to be slotted into a format request
            generating function.

        """
        range_dict = dict(sheetId=sheet_id)

        range_ = (*row_idxs, *col_idxs)
        non_nulls = 0
        for r in range_:
            non_nulls += 1 if r is not None else 0
        if non_nulls < 2:
            raise ValueError(
                'Must pass one or both of row_idxs, col_idxs.')

        start_row_idx, end_row_idx = row_idxs
        start_col_idx, end_col_idx = col_idxs
        # Must specify is not None because python interprets 0 as false.
        if start_row_idx is not None:
            range_dict['startRowIndex'] = start_row_idx
        if end_row_idx is not None:
            range_dict['endRowIndex'] = end_row_idx
        if start_col_idx is not None:
            range_dict['startColumnIndex'] = start_col_idx
        if end_col_idx is not None:
            range_dict['endColumnIndex'] = end_col_idx
        return range_dict

    @staticmethod
    def _build_color_dict(*rgb_vals) -> Dict[str, float]:
        """
        Quick method for building a color dictionary for use in a
        foreground, background, or font color dictionary.

        Args:
            *rgb_vals: Red, green, and blue values, in order. More than
                3 values will be ignored, default value is 0, so you
                only need to specify up to the last non-zero value.

        Returns: A dictionary containing RGB color names and values.

        """
        rgb_vals = list(rgb_vals)
        rgb = ['red', 'green', 'blue']
        rgb_vals += [0] * (3 - len(rgb_vals))
        return {
            k: v for k, v in dict(zip(rgb, rgb_vals)).items()
        }


def from_gsheet(
        sheet_name: str,
        s_api: SheetsAPI = None,
        sheet_title: str = None,
        drive_id: str = None) -> pd.DataFrame:
    """
    Creates a DataFrame from the first sheet of the passed Google Sheet
    name.

    Args:
        sheet_name: The exact name of the Google Sheet to pull from.
        s_api: A SheetsAPI object.
        sheet_title: A string, the name of the sheet within the Google
            Sheet to pull data from.
        drive_id: The id of the Shared Drive to search for the sheet in.

    Returns: A DataFrame.

    """
    # GeniusAccessor.from_file will pass fake .sheet extension.
    sheet_name = re.sub(r'\.sheet$', '', sheet_name)
    s_api = SheetsAPI() if s_api is None else s_api
    search_res = s_api.find_object(sheet_name, 'sheet', drive_id)
    sheet_id = search_res[0].get('id')
    if len(search_res) > 1:
        warnings.warn(
            f'Cannot find single exact match for {sheet_name}. '
            f'Taking data from the first match.'
        )
    rows = s_api.get_sheet_values(sheet_id, sheet_title)
    return pd.DataFrame(rows)


def write_gsheet(
        gsheet_name: str,
        df: pd.DataFrame,
        sheet_title: str = None,
        s_api: SheetsAPI = None,
        columns: list = None,
        parent_folder: str = None,
        drive_id: str = None,
        append: bool = False) -> tuple:
    """
    Uses the passed SheetsAPI object to write the passed DataFrame to a
    new Google Sheet.

    Args:
        gsheet_name: A string, the desired name of the new Google
            Sheet.
        df: A DataFrame.
        sheet_title: A string, the name of the sheet within the Google
            Sheet to save the data to. Default is the first sheet. If
            the sheet does not exist, it will be created.
        s_api: A SheetsAPI object.
        columns: The columns to use in the Google Sheet. If none, will
            just use the columns of the DataFrame.
        parent_folder: A string, the name of the folder to save the new
            Google Sheet to. Separate nested folders with /, as if it
            were a local file path.
        drive_id: The id of the Shared Drive to search for the folder
            path and to save to.
        append: If True and an existing google sheet is found, will
            write rows starting with the first blank row at the bottom
            of any existing rows.

    Returns: The number of cells changed by the output.

    """
    s_api = SheetsAPI() if s_api is None else s_api
    file_id, new_file = s_api.create_find(
        gsheet_name,
        'sheet',
        parent_folder,
        drive_id
    )
    df_rows = [*df.values.tolist()]
    sheet_md = s_api.get_sheet_metadata(file_id, sheet_title)
    if not new_file and append and sheet_md:
        start_row = sheet_md['row_limit'] + 1
    else:
        start_row = 1
        columns = list(df.columns) if columns is None else columns
        df_rows.insert(0, columns)
    result = s_api.write_values(file_id, df_rows, sheet_title, f'A{start_row}')
    return file_id, result


# def read_excel(file_name: str) -> dict:
#     """
#
#     Loops all sheets in an excel workbook and returns a dictionary
#     with sheet names as keys and the results of read_sheet() for
#     that corresponding sheet as the values.
#
#     Args:
#         file_name: The path to the excel file to read.
#
#     Returns: A dictionary containing sheet name keys and list of
#         list values representing the data in each sheet.
#
#     """
#     with xlrd.open_workbook(file_name) as wb:
#         names = wb.sheet_names()
#         result = dict()
#
#         for i in range(wb.nsheets):
#             result[names[i]] = read_sheet(wb.sheet_by_index(i))
#
#     return result


def get_output_template(file_path: str) -> list:
    """
    Builds a list of strings from a csv file's header row.

    Args:
        file_path: The file path of the csv file to read the header row
            from.

    Returns: A list containing the values of the file's header row.

    """
    return list(pd.read_csv(file_path).columns)
