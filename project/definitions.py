import dagster as dg

from project import data_extraction 


all_assets = dg.load_assets_from_modules([data_extraction])


defs = dg.Definitions(
    assets=[*all_assets, ]
)
