import csv


def read_csv(file_name):
    """
    Quickly reads a well-formatted csv file to a list of dictionaries.
    For best results, csv file must have no blank rows or columns and
    a complete header.

    Args:
        file_name: The path to the file to read.

    Returns: A list of dictionaries from the csv. Each row will have a
        dictionary with the header as its keys.

    """
    result = []
    with open(file_name, encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            result.append(row)
    return result
