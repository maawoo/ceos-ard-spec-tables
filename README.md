# ceos-ard-spec-tables

Companion to [S1_NRB](https://github.com/SAR-ARD/S1_NRB) in order to more efficiently handle 
[CEOS-ARD](https://ceos.org/ard/) specification metadata tables. Note that this repository is still _work in progress_.

## Installation
### conda
```bash
conda env create -f https://raw.githubusercontent.com/maawoo/ceos-ard-spec-tables/main/environment.yml
conda activate cast_env
pip install git+https://github.com/maawoo/ceos-ard-spec-tables.git
```

## Usage
### Load Excel file into a CASTMeta object
```python
from pathlib import Path
from cast.load import CASTMeta

file_dir = Path("./assets")
nrb_55 = CASTMeta(file_path=file_dir.joinpath("nrb", "CARD4L_METADATA-spec_NRB-v5.5.xlsx"))

# A dictionary of pandas DataFrames containing the cleaned up tables of the original Excel file is available
# as the attribute `data` of the CASTMeta object. The dictionary keys correspond to the sheet names.
nrb_55.data
```

### Create reports by comparing different objects
```python
from cast.compare import same_item_names_diff_vals

# Compare CASTMeta objects: Different versions of the same specification
compare_nrb = same_item_names_diff_vals(cast_1=nrb_50, cast_2=nrb_55)

# Compare CASTMeta objects: Different specifications
compare_nrb_orb = same_item_names_diff_vals(cast_1=nrb_55, cast_2=orb_10)
```
