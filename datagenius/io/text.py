import csv
import os

import xlrd
import pandas as pd


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


def build_template(file_path: str) -> list:
    """
    Builds a list of strings from a spreadsheet file's header row.

    Args:
        file_path: The file path of the file to read the header row
            from.

    Returns: A list containing the values of the file's header row.

    """
    data = pd.DataFrame.genius.from_file(file_path)
    return list(data.columns)
