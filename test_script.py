import pandas as pd
import numpy as np
from hdx.api.configuration import Configuration
from hdx.data.dataset import Dataset


Configuration.create(hdx_site='prod', user_agent= 'Palestine_HAPI_Data_Pipeline', hdx_read_only=True)
dataset = Dataset.read_from_hdx('hdx-hapi-pse')

if not dataset:
    print('item does not exist, look elsewhere')
else:
    print(f'Dataset found: {dataset["name"]}')
    print(f'Title {dataset["title"]}')

    resources = dataset.get_resources()
    print(f'\nFound {len(resources)} resources')
    
    for i, resource in enumerate(resources):  # limit to first 3
        print(f'\n--- Resource {i+1}: {resource["name"]} ---')
        try:
            url, path = resource.download('./raw_data')
            print(f'Downloaded from: {url}')
            print(f'Saved to: {path}')
        except Exception as e:
            print(f'Error downloading: {e}')