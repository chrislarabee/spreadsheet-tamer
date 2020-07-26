import pickle
import os

import pandas as pd
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request


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

    def create_folder(
            self,
            folder_name: str,
            drive_id: str = None) -> str:
        """
        Creates a folder of the passed name in the connected Google
        Drive.

        Args:
            folder_name: The name of the folder to be created.
            drive_id: The id of the shared drive to created the folder
                within.

        Returns: A string, the id of the newly created folder.

        """
        file_metadata = dict(
            name=folder_name,
            mimeType=self.google_obj_types['folder']
        )
        file = self.drive.files().create(
            body=file_metadata,
            fields='id'
        ).execute()
        return file.get('id')

    def delete_object(
            self,
            object_id: str,
            drive_id: str = None) -> None:
        """
        Deletes the passed Google Object ID from the connected Google
        Drive.

        Args:
            object_id: A Google Object ID.
            drive_id: The ID of the shared drive to target within.

        Returns: None

        """
        self.drive.files().delete(fileId=object_id).execute()

    def find_objects(
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
        query = f"name contains '{obj_name}'"
        if obj_type:
            query += f" and mimeType='{self.google_obj_types[obj_type]}'"
        kwargs = self._setup_drive_id_kwargs(drive_id)

        page_token = None
        results = []
        while True:
            response = self.drive.files().list(
                q=query,
                spaces='drive',
                fields='nextPageToken, files(id, name)',
                pageToken=page_token,
                **kwargs
            ).execute()
            for file in response.get('files', []):
                results.append(dict(
                    name=file.get('name'),
                    id=file.get('id')
                    )
                )
            page_token = response.get('nextPageToken', None)
            if page_token is None:
                break
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
