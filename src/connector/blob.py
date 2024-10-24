import os
import io
from dotenv import load_dotenv
from azure.storage.blob import BlobServiceClient
from azure.core.exceptions import HttpResponseError
from src.configuration import configuration

load_dotenv()

connect_str = os.getenv('connect_str')
container_name_blob = os.getenv('container_name_blob')

def upload_to_blob(string: str):
    
    # Initialize BlobServiceClient
    blob_service_client = BlobServiceClient.from_connection_string(connect_str)
    container_client = blob_service_client.get_container_client(container=container_name_blob)
      
    try:
        blob_client = container_client.upload_blob(name=configuration.Basefile_name+'.csv', data=io.BytesIO(string.encode('utf-8')), overwrite=True)
    except HttpResponseError as e:
        print(e)

def download_from_blob(blob_name):
    """Downloads given blob name from container specified in configuration.
    Downloaded file is saved to disk - 'temp_files/currentBackup.csv'
    """
    connection_string = os.getenv("connect_str")
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    container_name = os.getenv("container_name_blob")

    blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
    download_stream = blob_client.download_blob(encoding='UTF-8')
    current_csv_string = download_stream.readall()
    return current_csv_string