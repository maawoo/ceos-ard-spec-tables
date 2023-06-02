from pathlib import Path
import copy
import pandas as pd

OUT_DIR = Path.cwd().joinpath('reports')


def by_item_names(cast_1, cast_2, out_dir=None):
    """
    Compare two CASTMeta objects and find all rows that have the same item name but different values.
    
    Parameters
    ----------
    cast_1 : CASTMeta
        First CASTMeta object to compare.
    cast_2  : CASTMeta
        Second CASTMeta object to compare.
    out_dir : Path, optional
        Path to directory where output files will be saved. If not provided, defaults to 'reports' directory.
    
    Returns
    -------
    compare_dict : dict
        Dictionary of dataframes containing the rows that have the same item name but different values.
    """
    if out_dir is None:
        out_dir = OUT_DIR
    
    df_dict_1 = copy.deepcopy(cast_1.data)
    df_dict_2 = copy.deepcopy(cast_2.data)
    spec1 = cast_1.spec
    spec2 = cast_2.spec
    
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
        common_idx = df1.index.intersection(df2.index)
        
        # compare values of the common rows. `item` column is treated separately to be inserted back later on
        compare_item_col = df1.loc[common_idx, df1.columns[:1]].compare(df2.loc[common_idx, df2.columns[:1]],
                                                                        align_axis=0,
                                                                        keep_shape=True,
                                                                        keep_equal=True,
                                                                        result_names=(spec1, spec2))
        compare_main = df1.loc[common_idx, df1.columns[1:]].compare(df2.loc[common_idx, df2.columns[1:]],
                                                                    align_axis=0,
                                                                    keep_shape=True,
                                                                    keep_equal=False,
                                                                    result_names=(spec1, spec2))
        
        if not all(compare_item_col.index == compare_main.index):
            raise ValueError("Indices of compare_item_col and compare_main are not the same!")
        
        # re-insert column 'item' as first column in compare_main
        compare_main.insert(0, 'item', compare_item_col)
        
        # find and drop duplicate row-pairs
        dups = compare_main.duplicated(subset=compare_main.columns[1:], keep=False)
        compare_main.drop(compare_main.index[dups], inplace=True)
        
        # Reset index so spec is included as a column
        compare_main.reset_index(level=1, inplace=True)
        compare_main.rename(columns={"level_1": "spec"}, inplace=True)
        
        compare_dict[k] = compare_main
    
    with open(out_dir.joinpath(f"{spec1}_{spec2}__same_item_names_diff_vals.csv"), 'w') as f:
        i = 0
        header = True
        for key, df in compare_dict.items():
            if i > 0:
                header = False
            df.to_csv(f, header=header)
            i += 1
    
    return compare_dict

