import numpy as np
import pandas as pd
import requests
import dagster as dg
from hdx.api.configuration import Configuration
from hdx.data.dataset import Dataset
import os
from pathlib import Path


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

        project_root = Path(__file__).parent.parent
        download_dir = project_root/"projects"/"raw_data"/"hdx_hapi_data"
        download_dir.mkdir(parents= True, exist_ok= True)

        #preprocessing: converting dataset to parquet format

     

        print('Cleaning existing files...')
        for old_file in download_dir.glob('*'):
            if old_file.is_file():
                old_file.unlink()
                print(f'Deleted: {old_file.name}')

        resources = dataset.get_resources()

        for i, resource in enumerate(resources):
            resource_name = resource['name']
            print(f'\n--- Resource {i+1}: {resource["name"]} ---')

            #file_path = download_dir/resource_name      
            try:
                url, path = resource.download(str(download_dir))
                print(f'Downloaded from: {url}')
                print(f'saved to: {path}')
                print(f'Saved')

                #checking file format

                if path.endswith(('.xlsx', '.xls')):
                    print('Converting to csv')

                    #converting format to csv for optimization

                    files = pd.ExcelFile(path)
                    sheets = files.sheet_names
                    print(f'Found {len(sheets)} sheet(s): {sheets}')

                    #processing each sheets

                    for sheet_names in sheets:
                        print(f'\n  Processing sheet: {sheet_names}')

                        # Reading the sheet
                        df = pd.read_excel(path, sheet_names=sheet_names)
                        print(f'Rows: {len(df)}, Columns: {len(df.columns)}')

                       


                    converted_data = Path(files).stem + '.csv'
                    converted_data = files / converted_data




            except Exception as e:
                print(f'Download failed: {e}')




            


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
        
        project_root = Path(__file__).parent.parent
        download_dir = project_root/"projects"/"raw_data"/"acled"
        download_dir.mkdir(parents= True, exist_ok= True)

        print('Cleaning existing files...')
        for old_file in download_dir.glob('*'):
            if old_file.is_file():
                old_file.unlink()
                print(f'Deleted: {old_file.name}')

        resource = dataset.get_resources()

        for i, resources in enumerate(resource):
            try:
                url, path = resources.download(str(download_dir))
                print(f'data successfully downloaded from: {url}')
                print(f'data successfully saved to: {path}')
            except Exception as e:
                print(f'Download failed!!! {e}')




@dg.asset()
def civ_target_events_ext () -> None:

    """
    A weekly dataset providing the total number of reported civilian targeting events and fatalities broken down by country. 
    Civilian targeting events include violence against civilians events and explosions/remote violence events in which civilians were directly targeted.
    This dataset was sourced from the Armed Conflict Location & Event Data Project (ACLED), it is updated weekly

    """

    Configuration.create(hdx_site = 'prod', 
                         user_agent = 'civilian-targeting-events-and-fatalities',
                         hdx_read_only = True)
    dataset = Dataset.read_from_hdx('civilian-targeting-events-and-fatalities')

    if not dataset:
        print('dataset not found!!!, Look elsewhere')
    
    else:
        print(f'Dataset found!: {dataset["name"]}')
        print(f'Title: {dataset["title"]}')
        
        project_root = Path(__file__).parent.parent
        download_dir = project_root/"projects"/"raw_data"/"acled"
        download_dir.mkdir(parents= True, exist_ok= True)

        print('Cleaning existing files...')
        for old_file in download_dir.glob('*'):
            if old_file.is_file():
                old_file.unlink()
                print(f'Deleted: {old_file.name}')

        resource = dataset.get_resources()

        for i, resources in enumerate(resource):
            try:
                url, path = resources.download(str(download_dir))
                print(f'data successfully downloaded from: {url}')
                print(f'data successfully saved to: {path}')
            except Exception as e:
                print(f'Download failed!!! {e}')
