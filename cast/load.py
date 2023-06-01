import re
import pandas as pd

from cast.convert import convert_cast_df_dict


class CASTMeta(object):
    """
    Load CEOS-ARD Specification Table xlsx file into a dictionary of pandas DataFrames.
    
    Parameters
    ----------
    file_path : str or pathlib.Path
        Path to the Excel file.
    sheet_names : str or list of str, optional
        Name of the sheet(s) to load. If None, load all sheets. Default is ['General Metadata', 'Per-Pixel Metadata',
        'Radiometric Corrections', 'Geometric Corrections'].
    header : int, optional
        Row number to use as the column names. Default is 2.
    column_names : list of str, optional
        List of column names to use. Default is ['item', 'item_name', 'threshold_req', 'target_req', 'item_attr', 'type'].
    
    Attributes
    ----------
    raw : dict of pandas.DataFrame
        Dictionary of pandas DataFrames. Raw data from the Excel file.
    data : dict of pandas.DataFrame
        Dictionary of pandas DataFrames. Raw data converted to a workable format.
    spec : str
        Specification abbreviation and version.
    
    Examples
    --------
    >>> from pathlib import Path
    >>> from cast.load import CASTMeta
    >>>
    >>> file_dir = Path("./assets")
    >>> xlsx_nrb = file_dir.joinpath("nrb", "CARD4L_METADATA-spec_NRB-v5.0.xlsx")
    >>> nrb = CASTMeta(file_path=xlsx_nrb)
    >>> nrb.data['General Metadata']
    """
    def __init__(self, file_path, sheet_names=None, header=None, column_names=None):
        if not file_path.is_file():
            raise FileNotFoundError(f"File not found: {file_path}")
        if sheet_names is None:
            sheet_names = ['General Metadata', 'Per-Pixel Metadata', 'Radiometric Corrections', 'Geometric Corrections']
        if header is None:
            header = 2
        if column_names is None:
            column_names = ['item', 'item_name', 'threshold_req', 'target_req', 'item_attr', 'type']
        
        self.file = file_path
        self.sheets = sheet_names
        self.columns = column_names
        self.__header = header
        
        self.raw = self.load_xlsx()
        self.data = self.convert()
        self.spec = self.get_spec_and_version()
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
    
    def close(self):
        del self.data
    
    def get_spec_and_version(self):
        """
        Get specification abbreviation and version number from file path.
        
        Returns
        -------
        str
            The spec and version number of the file.
        """
        spec = self.file.parent.stem.upper()
        version = re.search(r'v\d\.\d{1,2}', self.file.name).group(0)
        
        return f"{spec}-{version}"
    
    def load_xlsx(self):
        """
        Load CEOS-ARD Specification xlsx file into a dictionary of pandas DataFrames.
        
        Returns
        -------
        dict of pandas.DataFrame
            Dictionary of pandas DataFrames.
        """
        return pd.read_excel(self.file, sheet_name=self.sheets, header=self.__header, names=self.columns)
    
    def convert(self):
        """
        Convert a dictionary of DataFrames containing CEOS-ARD metadata into a workable format.
        
        Returns
        -------
        dict of pandas.DataFrame
            Dictionary of converted DataFrames.
        """
        return convert_cast_df_dict(df_dict=self.raw)
