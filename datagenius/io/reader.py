import csv
import os

import xlrd

from datagenius.dataset import Dataset


def read_csv(file_name: str) -> dict:
    """
    Quickly reads a well-formatted csv file to a list of lists.
    Also, attempts to parse data as numeric and uses float
    data type if successful.

    Args:
        file_name: The path to the csv file to read.

    Returns: A dictionary containing a file name key and list of
        list values representing the data that file.

    """
    # Collects the unadorned file name (no root, no extensions).
    h, t = os.path.split(file_name)
    key, _ = os.path.splitext(t)
    result = []
    with open(file_name, newline='', encoding='utf-8') as f:
        for row in csv.reader(f):
            parsed_row = []
            for r in row:
                try:
                    fr = float(r)
                except ValueError:
                    parsed_row.append(r)
                else:
                    parsed_row.append(fr)
            result.append(parsed_row)
    return {key: Dataset(result)}


def read_excel(file_name: str) -> dict:
    """

    Loops all sheets in an excel workbook and returns a dictionary
    with sheet names as keys and the results of read_sheet() for
    that corresponding sheet as the values.

    Args:
        file_name: The path to the excel file to read.

    Returns: A dictionary containing sheet name keys and list of
        list values representing the data in each sheet.

    """
    with xlrd.open_workbook(file_name) as wb:
        names = wb.sheet_names()
        result = dict()

        for i in range(wb.nsheets):
            result[names[i]] = read_sheet(wb.sheet_by_index(i))

    return result


def read_file(file_name: str) -> dict:
    """
    Uses the appropriate reader function based on the extension
    of the passed file.

    Args:
        file_name: The path to the file to read.

    Returns: A dictionary containing keys that correspond to
        file names or sheets (as appropriate to the extension) and
        each file or sheet's data.

    """
    _, ext = os.path.splitext(file_name)

    read_funcs = {
        '.xls': read_excel,
        '.xlsx': read_excel,
        '.csv': read_csv
    }

    if ext not in read_funcs.keys():
        raise ValueError(f'read_file error: file extension must be '
                         f'one of {read_funcs.keys()}')
    else:
        return read_funcs[ext](file_name)


def read_sheet(sheet: xlrd.sheet) -> Dataset:
    """
    Reads all rows and columns in a Microsoft excel sheet and creates
    a list of lists with all values from the spreadsheet.

    Args:
        sheet: A xlrd sheet object.

    Returns: A list of lists, one for each row in the sheet,
        with indexes being the values from each cell in the row.

    """
    result = []
    for i in range(sheet.nrows):
        row = []
        for j in range(sheet.ncols):
            row.append(sheet.cell_value(i, j))
        result.append(row)
    return Dataset(result)
