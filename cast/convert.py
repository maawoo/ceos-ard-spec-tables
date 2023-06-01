import pandas as pd
import numpy as np


def truncate_and_ffill(df_dict):
    """
    Truncate beginning and end of each DataFrame in the dictionary and forward fill NaNs in the first column.
    
    Parameters
    ----------
    df_dict : dict of pandas.DataFrame
        Dictionary of pandas DataFrames.
    
    Returns
    -------
    dict of pandas.DataFrame
        Dictionary of truncated and forward filled DataFrames.
    """
    df_dict_copy = df_dict.copy()
    
    for k in list(df_dict_copy.keys()):
        df = df_dict_copy[k]
        
        # Get first valid index in column 0 (skip first row, which is the header)
        first_row = df.iloc[1:, 0].first_valid_index()
        
        # Get last valid index in column 0 and check for all-NaN rows after that index
        last_idx = df.iloc[:, 0].last_valid_index()
        if any(df.iloc[last_idx:, :].isna().all(axis=1)):
            # Get row before the last all-NaN row in the df. Otherwise, we might lose information for the last item.
            # Example: assets/CARD4L_METADATA-spec_NRB-v5.5.xlsx comments in the end of sheet 'General Metadata'!
            last_row = df[df.isna().all(axis=1)].tail(1).index[0] - 1
        else:
            last_row = df.index[-1]
        
        # Truncate and drop all-NaN rows
        #print(sum(df.isna().all(axis=1)))
        df = df.loc[first_row:last_row].dropna(how='all')
        
        # Forward fill NaNs in the first column and convert elements to strings
        df.iloc[:, 0].fillna(method='ffill', inplace=True)
        df.iloc[:, 0] = df.iloc[:, 0].apply(lambda x: str(x))
        
        # Replace empty strings with NaNs and save the modified DataFrame in the dictionary
        df.replace(r'^\s*$', np.NaN, regex=True, inplace=True)
        df_dict_copy[k] = df
    
    return df_dict_copy


def _remove_first_elem_per_row(df, col_start=None):
    """
    Iterate over rows and first checks that elements in columns starting from index `col_start` are consistent
    (same type and length). If consistent, it removes the first element of each tuple if len(tuple)>1 and if all first
    elements are NaN.
    
    Parameters
    ----------
    df : pandas.DataFrame
        DataFrame containing the tables.
    col_start : int, optional
        Column index to truncate from. The default is 0, which means that no columns are truncated.
    
    Returns
    -------
    pandas.DataFrame
        Modified DataFrame.
    """
    if col_start is None:
        col_start = 0
    
    n_rows = df.shape[0]
    for i in range(n_rows):
        row = df.iloc[i, col_start:]
        
        # Check that all elements are of the same type (tuple)
        if not all(row.apply(lambda x: isinstance(x, tuple))):
            raise ValueError(f"Row {i} (item {df.iloc[i, 0]}) contains non-tuple elements.")
        
        # Check that tuples in each row are of the same length
        lengths = row.apply(lambda x: len(x))
        if len(set(lengths)) > 1:
            raise ValueError(f"Tuples in row {i} (item {df.iloc[i, 0]}) have different lengths.")
        
        # Remove the first element of each tuple if len > 1 and if all first elements in the row are NaN
        if all(lengths > 1):
            first_elem_nan = all([pd.isna(elem[0]) for elem in row])
            if first_elem_nan:
                df.iloc[i, col_start:] = row.apply(lambda x: x[1:])
    
    return df


def compress_structure(df_dict):
    """
    Compress the structure of each DataFrame in the dictionary by grouping the table by `item` column and aggregating
    the rest of the columns into tuples. Then, two cleanup steps are performed:
    1. Only the first element of items in column `item_name` are kept.
    2. Subsequent columns are cleaned up by removing the first element of each tuple if len > 1 and if all first elements in
    the row are NaN.
    
    Parameters
    ----------
    df_dict : dict of pandas.DataFrame
        Dictionary of pandas DataFrames.
    
    Returns
    -------
    dict of pandas.DataFrame
        Dictionary of compressed DataFrames.
    """
    df_dict_copy = df_dict.copy()
    
    for k in list(df_dict_copy.keys()):
        df = df_dict_copy[k]
        
        col_item_name = df.columns.get_loc('item_name')
        col_start = col_item_name + 1
        if not col_item_name == 1:
            raise ValueError(f"Column 'item_name' is not at index 1 in DataFrame {k}.")
        
        # Group the table by item column and aggregate the rest of the columns into tuples
        df_grouped = df.groupby('item').agg(lambda x: tuple(x)).reset_index()
        
        # Cleanup
        df_grouped.iloc[:, col_item_name] = df_grouped.iloc[:, col_item_name].apply(lambda x: x[0] if x[0] else None)
        df_grouped = _remove_first_elem_per_row(df=df_grouped, col_start=col_start)
        
        df_dict_copy[k] = df_grouped
    
    return df_dict_copy


def convert_cast_df_dict(df_dict):
    """
    Convert a dictionary of DataFrames containing CEOS-ARD metadata into a workable format.
    
    Parameters
    ----------
    df_dict : dict of pandas.DataFrame
        Dictionary of pandas DataFrames.

    Returns
    -------
    dict of pandas.DataFrame
        Dictionary of converted DataFrames.
    """
    df_dict_copy = df_dict.copy()
    
    df_dict_copy_t = truncate_and_ffill(df_dict=df_dict_copy)
    df_dict_copy_t_c = compress_structure(df_dict=df_dict_copy_t)
    
    return df_dict_copy_t_c
