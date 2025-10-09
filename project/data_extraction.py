import numpy as np
import pandas as pd
import requests
import dagster as dg
from hdx.api.configuration import Configuration
from hdx.data.dataset import Dataset


@dg.asset()

def data_extract() -> None:
    Configuration.create(hdx_site = 'prod', 
                                  user_agent = 'Palestine_HAPI_Data_Pipeline', 
                                  hdx_read_only = True
                                  ) ### this line is used to establish connection to the hdx api
    
    dataset = Dataset.read_from_hdx('hdx-hapi-pse') ####this points to the dataset of interest 

    if not dataset:
        print('Dataset not found')
 
    else:
        print(f'Dataset Found!: {dataset["name"]}')
        print(f'Title: {dataset["title"]}')

        resources = dataset.get_resources()

        for i, resource in enumerate(resources):
            print(f'\n--- Resource {i+1}: {resource["name"]} ---')
            try:
                url, path = resource.download("./project/raw_data")
                print(f'Downloaded from: {url}')
                print(f'Saved to: {path}')
            except Exception as e:
                print(f'Download from failed {e}')



#data extract from acled site

@dg.asset()
def pse_acled_ext () -> None:

    """
    This asset is used to extract conflict data uploaed by ACLED
    """

    Configuration.create(hdx_site = 'prod', 
                         user_agent = 'Israel_genocide_on_PSE',
                         hdx_read_only = True)
    dataset = Dataset.read_from_hdx('palestine-acled-conflict-data')

    if not dataset:
        print('dataset not found!!!, Look elsewhere')
    
    else:
        print(f'Dataset found!: {dataset["name"]}')
        print(f'Title: {dataset["title"]}')

        resource = dataset.get_resources()

        for i, resources in enumerate(resource):
            try:
                url, path = resources.download('./project/raw_data/acled')
                print(f'data successfully downloaded from: {url}')
                print(f'data successfully saved to: {path}')
            except Exception as e:
                print(f'Download failed!!! {e}')
