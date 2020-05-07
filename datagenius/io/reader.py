import csv

import xlrd


def read_csv(file_name):
    """
    Quickly reads a well-formatted csv file to a list of lists.
    For best results, csv file must have no blank rows or columns and
    a complete header.

    Args:
        file_name: The path to the csv file to read.

    Returns: A list of lists, one for each row in the sheet,
        with indexes being the values from each cell in the row.

    """
    result = []
    with open(file_name, newline='', encoding='utf-8') as f:
        for row in csv.reader(f):
            result.append(row)
    return result


def read_sheet(sheet):
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


def read_excel(file_name):
    """

    Loops all sheets in an excel workbook and returns a dictionary
    with sheet names as keys and the results of read_sheet() for
    that corresponding sheet as the values.

    Args:
        file_name: The path to the excel file to read.

    Returns: A dictionary containing sheet name keys and list of
        list values representing the data in each sheet.

    """
    wb = xlrd.open_workbook(file_name)
    names = wb.sheet_names()
    result = dict()

    for i in range(wb.nsheets):
        result[names[i]] = read_sheet(wb.sheet_by_index(i))

    return result
