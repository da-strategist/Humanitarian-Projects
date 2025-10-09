import pandas as pd
import numpy as np
from hdx.api.configuration import Configuration
from hdx.data.dataset import Dataset


Configuration.create(hdx_site='prod', user_agent='A_Quick_Example', hdx_read_only=True)
dataset = Dataset.search_in_hdx('State of Palestine')

if not dataset:
    print('item does not exist, look elsewhere')
else:
    print(f'found {len(dataset)} in {dataset}')

    for i, dataset in enumerate(dataset[:3]):  # limit to first 3
        print(f"\nDataset {i+1}: {dataset['name']}")
        resources = dataset.get_resources()

        for resource in resources:
            url, path = resource.download("./raw_data")
            print(f'Resource url{url} downloaded to {path}')

