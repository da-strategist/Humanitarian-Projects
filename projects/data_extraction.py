import numpy as np
import pandas as pd
import requests
import dagster as dg
from hdx.api.configuration import Configuration
from hdx.data.dataset import Dataset
import os
from pathlib import Path


##############################
@dg.asset()
def data_extract() -> None:
    Configuration.create(hdx_site = 'prod', 
                                  user_agent = 'Palestine_HAPI_Data_Pipeline', 
                                  hdx_read_only = True
                                  ) ### this line is used to establish connection to the hdx api
    
    dataset = Dataset.read_from_hdx('hdx-hapi-pse') ####this points to the dataset of interest 

    #checking if dataset exists
    if not dataset:
        print('Dataset not found')
 
    else:
        print(f'Dataset Found!: {dataset["name"]}')
        print(f'Title: {dataset["title"]}')

        project_root = Path(__file__).parent.parent
        download_dir = project_root/"projects"/"raw_data"/"hdx_hapi_data"
        download_dir.mkdir(parents= True, exist_ok= True)

        #preprocessing: removing existing version of the file

        print('Cleaning existing files...')
        for old_file in download_dir.glob('*'):
            if old_file.is_file():
                old_file.unlink()
                print(f'Deleted: {old_file.name}')

        resources = dataset.get_resources()
        converted_files = []
        total_sheets_processed = 0

        for i, resource in enumerate(resources):
            resource_name = resource['name']
            print(f'\n--- Resource {i+1}: {resource["name"]} ---')

            try:
                url, path = resource.download(str(download_dir))
                print(f'Downloaded from: {url}')
                print(f'Saved to: {path}')

                #checking file format
                if path.endswith(('.xlsx', '.xls')):
                    print('Converting to csv')

                    #converting format to csv for optimization
                    files = pd.ExcelFile(path)
                    sheets = files.sheet_names
                    print(f'Found {len(sheets)} sheet(s): {sheets}')

                    #processing each sheets
                    for sheet_name in sheets:  
                        print(f'\n  Processing sheet: {sheet_name}')

                        # Reading the sheet
                        df = pd.read_excel(path, sheet_name=sheet_name)  
                        print(f'Rows: {len(df)}, Columns: {len(df.columns)}')

                        # Create CSV filename with sheet name
                        filename = Path(path).stem
                        if len(sheets) > 1:  
                            # Include sheet name if multiple sheets
                            csv_filename = f'{filename}_{sheet_name}.csv'  
                        else:
                            # Just use base filename if only one sheet
                            csv_filename = f'{filename}.csv'
                        
                        csv_filename = csv_filename.replace('/', '_').replace('\\', '_').replace(':', '_')
                        csv_path = download_dir / csv_filename
                    
                        # Save as CSV
                        df.to_csv(csv_path, index=False)
                        print(f'CSV saved to: {csv_path}')
                        
                        converted_files.append(str(csv_path))
                        total_sheets_processed += 1
                    
                    # Optionally remove original Excel file to save space (moved outside the sheet loop)
                    Path(path).unlink()
                    print(f'Removed original Excel file: {path}')
                
                else:
                    print(f'File is not Excel format: {path}')
                    converted_files.append(path)
                
            except Exception as e:
                print(f'Download/conversion failed: {e}')
                raise
    
        print(f"\nProcessing complete!")
        print(f"Total sheets processed: {total_sheets_processed}")
        print(f"Total CSV files created: {len(converted_files)}")
#############################
                 

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
        download_dir = project_root/"projects"/"raw_data"/"acled_01"
        download_dir.mkdir(parents= True, exist_ok= True)

        print('Cleaning existing files...')
        for old_file in download_dir.glob('*'):
            if old_file.is_file():
                old_file.unlink()
                print(f'Deleted: {old_file.name}')

        resource = dataset.get_resources()
        converted_files = []
        total_processed_files = 0

        for i, resources in enumerate(resource):
            try:
                url, path = resources.download(str(download_dir))
                print(f'data successfully downloaded from: {url}')
                print(f'data successfully saved to: {path}')

                #checking file type/format
                if path.endswith(('.xlsx', '.xls')):
                    print('File is in excel format')

                    file = pd.ExcelFile(path)
                    sheets = file.sheet_names

                    #next we process individual sheets
                    for sheet_name in sheets:
                        data = pd.read_excel(path, sheet_name=sheet_name)
                       
                        #next we create a csv filename to hold individual sheets
                        filename = Path(path).stem

                        #we check if the file contains multiple sheets
                        if len(sheets) > 1:
                            csv_filename = f'{filename}_{sheet_name}.csv'
                        
                        else:
                            csv_filename = f'{filename}.csv'
                        
                        csv_path = download_dir / csv_filename
                        
                        #now we save as csv
                        data.to_csv(csv_path, index= False)
                        print(f'file saved to {csv_path}')

                        #next we append each file 
                        converted_files.append(str(csv_path))
                        total_processed_files += 1
                    
                    Path(path).unlink()
                    print('old file deleted')
                
                else: 
                    print('file not in excel format')
                    converted_files.append(path)

            except Exception as e:
                print(f'Download failed!!! {e}')
                raise
        
        print(f"\nProcessing complete!")
        print(f"Total sheets processed: {total_processed_files}")
        print(f"Total CSV files created: {len(converted_files)}")




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
        converted_files = []
        total_processed_files = 0

        for i, resources in enumerate(resource):
            try:
                url, path = resources.download(str(download_dir))
                print(f'data successfully downloaded from: {url}')
                print(f'data successfully saved to: {path}')

                #now we check file type / format

                if path.endswith(('.xlsx', '.xls')):

                    filename = pd.ExcelFile(path)
                    sheets = filename.sheet_names

                    for sheet_name in sheets:
                        data = pd.read_excel(path, sheet_name= sheet_name)
                        
                        filename = Path(path).stem

                        if len(sheets) > 1:
                            csv_file = f'{filename}_{sheet_name}.csv' 

                        else:
                            csv_file = f'{filename}.csv'
                        
                        csv_path = download_dir / csv_file
                        
                        data.to_csv(csv_path, index= False)
                        print(f'file saved to {csv_path}')

                        converted_files.append(str(path))
                        total_processed_files += 1
                    
                    Path(path).unlink()
                    print('old file deleted')

                else:
                    print('file not in excel format')
                    converted_files.append(path)
                
            except Exception as e:
                print(f'Download failed!!! {e}')
                raise

        print(f"\nProcessing complete!")
        print(f"Total sheets processed: {total_processed_files}")
        print(f"Total CSV files created: {len(converted_files)}")

                    


                    


            