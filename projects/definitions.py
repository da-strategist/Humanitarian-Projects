import dagster as dg

from projects import data_extraction, data_load


all_assets = dg.load_assets_from_modules([data_extraction, data_load])


defs = dg.Definitions(
    assets=[*all_assets, ]
)
