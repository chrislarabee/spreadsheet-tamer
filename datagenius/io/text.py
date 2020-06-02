import csv
import os

import xlrd


def read_csv(file_name: str) -> dict:
    """
    Quickly reads a well-formatted csv file to a list of lists.

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
            result.append(row)
    return {key: result}


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


def read_sheet(sheet: xlrd.sheet) -> list:
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
    return result


def build_template(file_path: str) -> list:
    """
    Builds a list of strings from a spreadsheet file's header row.

    Args:
        file_path: The file path of the file to read the header row
            from.

    Returns: A list containing the values of the file's header row.

    """
    data = read_file(file_path)
    key = list(data.keys())[0]
    return data[key][0]


def write_csv(file_path: str, data: list, header: list) -> None:
    """
    Simple csv writing function.

    Args:
        file_path: A valid file path to write to.
        data: A list of dictionaries.
        header: A list of strings to use as the header row.

    Returns: None

    """
    if os.path.exists(file_path):
        os.remove(file_path)
    with open(file_path, 'w', newline='') as f:
        w = csv.DictWriter(f, fieldnames=header)
        w.writeheader()
        for d in data:
            w.writerow(d)
