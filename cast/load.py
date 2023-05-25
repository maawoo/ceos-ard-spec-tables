import pandas as pd
from pathlib import Path


def load_card_xlsx(path, sheet_name=None, header=None, names=None):
    """
    Load specific sheets of a CEOS-ARD Specification xlsx file into a dictionary of pandas DataFrames.
    
    Parameters
    ----------
    path : str or Path
        Path to the Excel file.
    sheet_name : str or list of str, optional
        Name of the sheet(s) to load. If None, load all sheets. Default is ['General Metadata', 'Per-Pixel Metadata',
        'Radiometric Corrections', 'Geometric Corrections'].
    header : int, optional
        Row number to use as the column names. Default is 2.
    names : list of str, optional
        List of column names. Default is ['item', 'item_name', 'threshold_req', 'target_req', 'item_attr', 'type'].
    
    Returns
    -------
    dict of pandas.DataFrame
        Dictionary of pandas DataFrames.
    """
    if sheet_name is None:
        sheet_name = ['General Metadata', 'Per-Pixel Metadata', 'Radiometric Corrections', 'Geometric Corrections']
    if header is None:
        header = 2
    if names is None:
        names = ['item', 'item_name', 'threshold_req', 'target_req', 'item_attr', 'type']
    
    return pd.read_excel(path, sheet_name=sheet_name, header=header, names=names)
