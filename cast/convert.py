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
        
        # Get first row that is not NaN in the first column
        first_row = df[df.iloc[:, 0].notna()].index[0]
        
        # Get row before the last all-NaN row in the df. Otherwise, we might lose information for the last item.
        last_row = df[df.isna().all(axis=1)].tail(1).index[0] - 1
        
        # Truncate and forward fill NaNs in the first column
        df = df.loc[first_row:last_row].dropna(how='all')
        df.iloc[:, 0].fillna(method='ffill', inplace=True)
        
        # Replace empty strings with NaNs and save the modified DataFrame in the dictionary
        df.replace(r'^\s*$', np.NaN, regex=True, inplace=True)
        df_dict_copy[k] = df
    
    return df_dict_copy


def _remove_first_elem_per_row(df):
    """
    This function iterates over the rows of a DataFrame and for columns 2 - 5 (here: 'threshold_req', 'target_req',
    'item_attr' and 'type') it removes the first element of each list if len(list)>1 and if all first elements are NaN.

    Parameters
    ----------
    df : pandas.DataFrame
        DataFrame containing the tables.

    Returns
    -------
    pandas.DataFrame
        Modified DataFrame.
    """
    n_rows = df.shape[0]
    for i in range(n_rows):
        # Get columns 2-5 of each row
        row = df.iloc[i, 2:6]
        
        # Check that all elements in each row are lists
        if not all(row.apply(lambda x: isinstance(x, list))):
            raise ValueError(f"Row {i} (item {df.iloc[i, 0]}) contains non-list elements.")
            # or continue?
        
        # Check that lists in each row are of the same length
        lengths = row.apply(lambda x: len(x))
        if len(set(lengths)) > 1:
            raise ValueError(f"Lists in {i} (item {df.iloc[i, 0]}) have different lengths.")
        
        # columns 2-5: Remove the first element of each list (len > 1) if all first elements in the row are NaN
        if all(lengths > 1):
            first_elem_nan = all([pd.isna(elem[0]) for elem in row])
            if first_elem_nan:
                df.iloc[i, 2:6] = row.apply(lambda x: x[1:])
    
    return df


def compress_structure(df_dict):
    """
    Compress the structure of each DataFrame in the dictionary by grouping the table by item column and aggregating
    the rest of the columns into lists. Then, remove the first element of each list (len > 1) if all first elements in
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
        
        # Group the table by item column and aggregate the rest of the columns into lists
        df_grouped = df.groupby('item').agg(lambda x: x.tolist()).reset_index()
        
        # column 1: Only keep the first element of the list
        df_grouped.iloc[:, 1] = df_grouped.iloc[:, 1].apply(lambda x: x[0] if x[0] else None)
        
        # columns 2-5: Remove the first element of each list (len > 1) if all first elements in the row are NaN
        df_grouped = _remove_first_elem_per_row(df=df_grouped)
        
        df_dict_copy[k] = df_grouped
    
    return df_dict_copy


def convert_card_df_dict(df_dict):
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
