import pandas as pd
import numpy as np
import openpyxl
from pathlib import Path
import plotly

# NOTE: The original code omitted the .xlsx extension which caused a FileNotFoundError
# when trying to read the Excel file. Use a resolved relative path so the script
# works when executed from the repository root.
#data = Path(__file__).resolve().parents[1] / "raw_data" / "acled" / "civilian-targeting-events-and-fatalities_as-of-2025-10-17.xlsx"

data_01 = "./projects/raw_data/acled/civilian-targeting-events-and-fatalities_as-of-2025-10-17_HRP_1.csv"
data_02 = "./projects/raw_data/acled/civilian-targeting-events-and-fatalities_as-of-2025-10-17_HRP_2.csv"

civ_crisis_data_01 = pd.read_csv(data_01)
civ_crisis_data_02 = pd.read_csv(data_02)
#print(civ_crisis_data.head())
"""
case_count = civ_crisis_data_01['Country'].value_counts().reset_index(name='case_count')
print(case_count)

case_count_02 = civ_crisis_data_02['Country'].value_counts().reset_index(name= 'case_count')
print(case_count_02)

"""
# total casualties for PSE

pse_fatalities = civ_crisis_data_01.loc[civ_crisis_data_01['Country']== 'Palestine',
                                    ['Country', 'Month', 'Year'
                                      ,'Events', 'Fatalities']]

total_fatalities = pse_fatalities.groupby(['Country', 'Year'])[['Events','Fatalities']].sum().reset_index()
print(total_fatalities)



#events by location

