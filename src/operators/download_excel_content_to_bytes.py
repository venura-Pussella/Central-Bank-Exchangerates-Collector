import requests
import pandas as pd
from io import BytesIO

# Function to download Excel content as bytes
def download_excel_content(excel_url):
    response = requests.get(excel_url)
    response.raise_for_status()  # Ensure the download was successful
    return BytesIO(response.content)

# Function to convert the Excel content into a Pandas DataFrame
def read_excel_to_dataframe(excel_content):
    return pd.read_excel(excel_content)

