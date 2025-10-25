import pandas as pd
import numpy as np
import openpyxl
from pathlib import Path

# NOTE: The original code omitted the .xlsx extension which caused a FileNotFoundError
# when trying to read the Excel file. Use a resolved relative path so the script
# works when executed from the repository root.
#data = Path(__file__).resolve().parents[1] / "raw_data" / "acled" / "civilian-targeting-events-and-fatalities_as-of-2025-10-17.xlsx"

data = "./projects/raw_data/acled/civilian-targeting-events-and-fatalities_as-of-2025-10-17.xlsx"

civ_crisis_data = pd.read_excel(data, sheet_name= 'HRP_1')
print(civ_crisis_data)