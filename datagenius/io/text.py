import pickle
import os
import warnings
import re

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

    def get_sheets(self, sheet_id: str) -> list:
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
            sheet_title: str = None) -> dict:
        """
        Retrieves metadata about the first sheet in the Google Sheet
        corresponding to the passed sheet_id.

        Args:
            sheet_id: A string, the id of a Google Sheet.
            sheet_title: A string, the name of the sheet within the
                Google Sheet to get metadata for. If none, metadata for
                the first sheet will be used.

        Returns: A dictionary containing information about the data in
            the passed sheet.

        """
        results = dict()
        raw = self.get_sheets(sheet_id)
        s_idx = 0
        if sheet_title is not None:
            s_idx = self.check_sheet_titles(sheet_title, sheets=raw)
        sheet = raw[s_idx]
        results['title'] = sheet['properties']['title']
        first_row = sheet['data'][0]['rowData']
        last_row_idx = len(first_row)
        last_col_idx = max([len(e['values']) for e in first_row if e])
        results['row_limit'] = last_row_idx
        results['col_limit'] = last_col_idx
        return results

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
    @property
    def auto_dim_size(self):
        """

        Returns: Base dictionary for automatically sizing row or column
            height/width.

        """
        return dict(
            autoResizeDimensions=dict(
                dimensions=dict()
            )
        )

    @property
    def delete_dim(self):
        """

        Returns: Base dictionary for deleting rows or columns.

        """
        return dict(
            deleteDimension=dict(
                range=dict()
            )
        )

    @property
    def number_fmt(self):
        return

    @property
    def acct_fmt(self):
        return 'NUMBER', '_($* #,##0.00_);_($* (#,##0.00);_($* "-"??_);_(@_)'

    def __init__(self):
        """
        This object contains methods for creating a list of formatting
        requests and row/column operation requests that the SheetsAPI
        object can process via the batch_update method.
        """
        self.requests: list = []

    def auto_column_width(
            self,
            start_col: int,
            end_col: int,
            sheet_id: int = 0):
        """
        Adds an autoResizeDimensions request to the GSheetFormatting
            object's requests queue.

        Args:
            start_col: The 0-initial index of the first column to auto-
                resize.
            end_col: The 0-initial index of the last column to auto-
                resize.
            sheet_id: The index of the sheet to delete rows from,
                default is 0, the first sheet.

        Returns: self

        """
        request = self.auto_dim_size
        request['autoResizeDimensions']['dimensions'] = self._build_dims_dict(
            sheet_id, 'COLUMNS', start_col, end_col
        )
        self.requests.append(request)
        return self

    def insert_rows(self, num_rows: int, sheet_id: int = 0, at_row: int = 0):
        """
        Adds an insertDimension request to add more rows to the
            GSheetFormatting object's requests queue.

        Args:
            num_rows: The # of rows to insert.
            sheet_id: The index of the sheet to delete rows from,
                default is 0, the first sheet.
            at_row: The 0-initial index of the row to start inserting
               at.

        Returns: self.

        """
        request = self._insert_dims(
            sheet_id, 'ROWS', at_row, at_row + num_rows)
        self.requests.append(request)
        return self

    def delete_rows(self, start_row: int, end_row: int, sheet_id: int = 0):
        """
        Adds a deleteDimension request to the GSheetFormatting object's
        requests queue.

        Args:
            start_row: The 0-initial index of the first row to delete.
            end_row: The 0-initial index of the last row to delete.
            sheet_id: The index of the sheet to delete rows from,
                default is 0, the first sheet.

        Returns: self.

        """
        request = self.delete_dim
        request['deleteDimension']['range'] = self._build_dims_dict(
            sheet_id, 'ROWS', start_row, end_row
        )
        self.requests.append(request)
        return self

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
            style: (str, tuple) = None,
            sheet_id: int = 0):
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
            sheet_id: The index of the sheet to change cells in,
                default is 0, the first sheet.

        Returns: self.

        """
        text_format = dict()
        if size:
            text_format['fontSize'] = size
        if style:
            style = u.tuplify(style)
            for s in style:
                text_format[s] = True
        request = self._user_entered_fmt(
            dict(textFormat=text_format),
            row_idxs,
            col_idxs,
            sheet_id
        )
        request['fields'] = 'userEnteredFormat(textFormat)'
        self.requests.append(request)
        return self

    def apply_nbr_format(
            self,
            fmt_property: tuple,
            row_idxs: tuple = (None, None),
            col_idxs: tuple = (None, None),
            sheet_id: int = 0):
        """
        Adds a numberFormat request to the GSheetFormatting object's
        request queue.

        Args:
            fmt_property: A _fmt property from this object (like
                acct_fmt)
            row_idxs: A tuple of the start and end rows to apply number
                formatting to.
            col_idxs: A tuple of the start and end columns to apply
                number formatting to.
            sheet_id: The index of the sheet to change cells in,
                default is 0, the first sheet.

        Returns: self.

        """
        t, p = fmt_property
        nbr_format = dict(type=t, pattern=p)

        request = self._user_entered_fmt(
            dict(numberFormat=nbr_format),
            row_idxs,
            col_idxs,
            sheet_id
        )
        request['fields'] = 'userEnteredFormat.numberFormat'
        self.requests.append(request)
        return self

    @classmethod
    def _user_entered_fmt(
            cls,
            fmt_dict: dict,
            row_idxs: tuple = (None, None),
            col_idxs: tuple = (None, None),
            sheet_id: int = 0) -> dict:
        """
        Quick method for creating a userEnteredFormat request
        dictionary.

        Args:
            fmt_dict: A dictionary containing a format key and any
                desired formatting information.
            row_idxs: A tuple of the start and end rows to apply
                formatting to.
            col_idxs: A tuple of the start and end columns to apply
                formatting to.
            sheet_id: The index of the sheet to change cells in,
                default is 0, the first sheet.

        Returns: A dictionary request to alter formatting of a Google
            Sheet.

        """
        range_ = (*row_idxs, *col_idxs)
        non_nulls = 0
        for r in range_:
            non_nulls += 1 if r is not None else 0
        if non_nulls < 2:
            raise ValueError(
                'Must pass one or both of row_idxs, col_idxs.')

        request = dict(
            **cls._build_repeat_cell_dict(
                sheet_id, *range_
            ),
            cell=dict(
                userEnteredFormat=dict(**fmt_dict)
            )
        )
        return request

    @staticmethod
    def _build_repeat_cell_dict(
            sheet_id: int = 0,
            start_row_idx: int = None,
            end_row_idx: int = None,
            start_col_idx: int = None,
            end_col_idx: int = None) -> dict:
        """
        Quick method for building a range dictionary for use in a
        request dictionary wrapper intended to change cell formatting or
        contents (like changing font, borders, background, contents,
        etc).

        Args:
            sheet_id: The index of the sheet to build a range for,
                default is 0, the first sheet.
            start_row_idx: The 0-initial index of the first row to
                target for formatting.
            end_row_idx: The 0-initial index of the last row to target
                for formatting.
            start_col_idx: The 0-initial index of the first column to
                target for formatting.
            end_col_idx: The 0-initial index of the last column to
                target for formatting.

        Returns: A dictionary ready to be slotted into a format request
            generating function.

        """
        range_ = dict(sheetId=sheet_id)
        # Must specify is not None because python interprets 0 as false.
        if start_row_idx is not None:
            range_['startRowIndex'] = start_row_idx
        if end_row_idx is not None:
            range_['endRowIndex'] = end_row_idx
        if start_col_idx is not None:
            range_['startColumnIndex'] = start_col_idx
        if end_col_idx is not None:
            range_['endColumnIndex'] = end_col_idx
        return dict(repeatCell=dict(range=range_))


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
        drive_id: str = None) -> tuple:
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

    Returns: The number of cells changed by the output.

    """
    s_api = SheetsAPI() if s_api is None else s_api
    p_folder_id = None
    if parent_folder:
        search_res = s_api.find_object(
            parent_folder,
            'folder',
            drive_id
        )
        if len(search_res) == 1:
            p_folder_id = search_res[0].get('id')
        else:
            warnings.warn(
                f'Cannot find single exact match for {parent_folder}. '
                f'Saving {gsheet_name} to root Drive.'
            )
    search_res = s_api.find_object(
        gsheet_name,
        'sheet',
        drive_id
    )
    if len(search_res) > 1:
        for result in search_res:
            if result.get('parents')[0] == p_folder_id:
                file_id = result.get('id')
                break
        else:
            raise ValueError(
                f'Cannot find {gsheet_name} in {parent_folder}')
    elif len(search_res) == 1:
        file_id = search_res[0].get('id')
    else:
        file_id = s_api.create_object(gsheet_name, 'sheet', p_folder_id)
    df_rows = [*df.values.tolist()]
    columns = list(df.columns) if columns is None else columns
    df_rows.insert(0, columns)
    result = s_api.write_values(file_id, df_rows, sheet_title)
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
