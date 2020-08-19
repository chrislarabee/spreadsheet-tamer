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
        return dict(
            autoResizeDimensions=dict(
                dimensions=None
            )
        )

    @property
    def delete_dim(self):
        return dict(
            deleteDimension=dict(
                range=None
            )
        )

    @property
    def insert_dims(self):
        return dict(
            insertDimension=dict(
                range=dict(),
                inheritFromBefore=False
            )
        )

    def __init__(self, file_id: str = None):
        self.file_id: str = file_id
        self.requests: list = []

    def set_file(self, file_id: str):
        self.file_id = file_id
        return self

    def insert_rows(self, num_rows: int, at_row: int = 1):
        request = self.insert_dims
        request['insertDimension']['range'] = self._build_dims_dict(
            self.file_id, 'ROWS', at_row - 1, at_row - 1 + num_rows)
        self.requests.append(request)
        return self

    @staticmethod
    def _build_dims_dict(*vals):
        d = dict(
            sheetId=None,
            dimension=None,
            startIndex=None,
            endIndex=None
        )
        return dict(zip(d.keys(), vals))


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
    sheet_md = s_api.get_sheet_metadata(sheet_id, sheet_title)
    r = sheet_title + '!' if sheet_title else ''
    last_col_alpha = u.gen_alpha_keys(sheet_md['col_limit'])
    col_letter = last_col_alpha[sheet_md['col_limit'] - 1]
    result = s_api.sheets.spreadsheets().values().get(
        spreadsheetId=sheet_id,
        range=f"{r}A1:{col_letter}{sheet_md['row_limit']}"
    ).execute()
    rows = result.get('values', [])
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
    df_rows = df.values.tolist()
    columns = list(df.columns) if columns is None else columns
    df_rows.insert(0, columns)
    if sheet_title is not None:
        r = sheet_title + '!'
        s = s_api.check_sheet_titles(sheet_title, sheet_id=file_id)
        if s is None:
            s_api.add_sheet(file_id, title=sheet_title)
    else:
        r = ''
    result = s_api.sheets.spreadsheets().values().update(
        spreadsheetId=file_id,
        range=r + 'A1',
        valueInputOption='USER_ENTERED',
        body=dict(values=df_rows)
    ).execute()
    return file_id, (result.get('updatedRows'),
                     result.get('updatedColumns'))


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
