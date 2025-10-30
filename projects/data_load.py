import dagster as dg
import pandas as pd
import numpy as np
from azure.storage.blob import BlobClient, BlobServiceClient
from azure.identity import DefaultAzureCredential
from projects import constant 
from io import BytesIO
from pathlib import Path



@dg.asset(deps=["data_extract", "pse_acled_ext", "civ_target_events_ext"])
def acled_upload_to_azure() -> None:
    """
    Upload CSV files directly from disk to Azure Blob Storage (memory efficient).
    """
    connection_string = constant.connection_string
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    
    project_root = Path(__file__).parent.parent
    raw_data_dir = project_root / "projects" / "raw_data"
    
    folders_to_upload = ["acled", "acled_01", "hdx_hapi_data"]
    container_name = 'conflictprojectdatasets'
    uploaded_files = []
    
    for folder_name in folders_to_upload:
        folder_path = raw_data_dir / folder_name
        
        if not folder_path.exists():
            print(f"Warning: Folder not found - {folder_path}")
            continue
        
        print(f"\n--- Processing folder: {folder_name} ---")
        csv_files = list(folder_path.glob("*.csv"))
        
        for csv_file in csv_files:
            try:
                blob_name = f"{folder_name}/{csv_file.name}"
                
                # Upload directly from file (more efficient)
                blob_client = blob_service_client.get_blob_client(
                    container=container_name,
                    blob=blob_name
                )
                
                with open(csv_file, "rb") as data:
                    blob_client.upload_blob(data, overwrite=True)
                
                file_size = csv_file.stat().st_size / 1024 / 1024  # MB
                print(f"✓ Uploaded: {blob_name} ({file_size:.2f} MB)")
                uploaded_files.append(blob_name)
                
            except Exception as e:
                print(f"✗ Failed to upload {csv_file.name}: {e}")
                raise
    
    print(f"\n=== Upload Complete ===")
    print(f"Total files uploaded: {len(uploaded_files)}")



    



