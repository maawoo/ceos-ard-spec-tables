from pathlib import Path
import copy
import pandas as pd

OUT_DIR = Path.cwd().joinpath('reports')


def same_item_names_diff_vals(card_1, card_2, out_dir=None):
    """
    Compare two CARDMeta objects and find all rows that have the same item name but different values.
    
    Parameters
    ----------
    card_1 : CARDMeta
        First CARDMeta object to compare.
    card_2  : CARDMeta
        Second CARDMeta object to compare.
    out_dir : Path, optional
        Path to directory where output files will be saved. If not provided, defaults to 'reports' directory.
    
    Returns
    -------
    compare_dict : dict
        Dictionary of dataframes containing the rows that have the same item name but different values.
    """
    if out_dir is None:
        out_dir = OUT_DIR
    
    df_dict_1 = copy.deepcopy(card_1.data)
    df_dict_2 = copy.deepcopy(card_2.data)
    spec1 = card_1.spec
    spec2 = card_2.spec
    
    # check that both df_dicts have same keys
    key_diff = set(df_dict_1.keys()).symmetric_difference(set(df_dict_2.keys()))
    if len(key_diff) > 0:
        raise ValueError(f"Both dicts must have the same keys. Found differences: {key_diff}")
    
    compare_dict = {}
    for k in list(df_dict_1.keys()):
        df1 = df_dict_1[k]
        df2 = df_dict_2[k]
        
        # set index to column 'item_name'
        df1.set_index('item_name', inplace=True)
        df2.set_index('item_name', inplace=True)
        
        # find common rows based on index
        common = df1.index.intersection(df2.index)
        
        # compare the values of the common rows
        compare_common = df1.loc[common].compare(df2.loc[common], align_axis=0,
                                                 keep_shape=True, keep_equal=True, result_names=(spec1, spec2))
        
        # find and drop duplicate rows
        dups = compare_common.reset_index().iloc[:, 2:].duplicated(keep=False)
        compare_common.drop(compare_common.index[dups], inplace=True)
        
        # Reset index so spec is included as a column, then set index back to 'item'
        compare_common.reset_index(level=1, inplace=True)
        compare_common.rename(columns={"level_1": "spec"}, inplace=True)
        
        compare_dict[k] = compare_common
    
    # create xlsx from dict of dfs using key as the sheet name
    with pd.ExcelWriter(out_dir.joinpath(f"same_item_names_diff_vals__{spec1}_{spec2}.xlsx")) as writer:
        for k in list(compare_dict.keys()):
            compare_dict[k].to_excel(writer, sheet_name=k)
    
    return compare_dict

