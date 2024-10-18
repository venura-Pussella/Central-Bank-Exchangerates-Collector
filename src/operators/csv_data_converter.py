import io
import csv
import pandas
import os
from src.connector import blob
from src.configuration import configuration

def downloadExistingCSVFromBlobAndGetDataframe() -> pandas.DataFrame:
    """Downloads the currently cloud-backed-up csv to a temporary path. Converts it to a dataframe and returns it.
    """
    df:pandas.DataFrame = None
    try:
        blob_name = configuration.Basefile_name + '.csv'
        blob.download_from_blob(blob_name)
        temporary_filepath = 'temp_files/currentBackup.csv'
        df = None
        if os.path.exists(temporary_filepath):
            df = pandas.read_csv(temporary_filepath)
    except Exception as e:
        print(e)
    return df
    

def getNewRowsOnlyAndSaveToCSV(cloudDF, exchange_rates_df: pandas.DataFrame) -> pandas.DataFrame:
    """Takes the current dataframe, and extracts only the new rows, and returns it as a dataframe. The new rows are saved to the temporary filepath.
    ### Args:
        cloudDF: dataframe corresponding to the csv backed up to the cloud
        exchange_rates_df: dataframe containing data extracted from the central bank excel file
    ### Returns: 
    dataframe with only the new data
    """
    print('getNewRowsOnlyAndSaveToCSV Called')
    if not isinstance(cloudDF, pandas.DataFrame):
        print('getNewRowsOnlyAndSaveToCSV IF')
        new_exchange_rates_df = exchange_rates_df
    else:
        print('getNewRowsOnlyAndSaveToCSV ELSE')
        cloudDF_numOfRows = cloudDF.shape[0]
        cloudDFLastDate = str(cloudDF.iloc[cloudDF_numOfRows - 1].iloc[0])

        lastCommonRowIndex = 0
        exchange_rates_df_numOfRows =  exchange_rates_df.shape[0]
        for i in range(exchange_rates_df_numOfRows - 1, -1, -1): 
            currentDate = str(exchange_rates_df.iloc[i].iloc[0])
            if currentDate == cloudDFLastDate:
                lastCommonRowIndex = i
                break

        new_exchange_rates_df = exchange_rates_df.iloc[lastCommonRowIndex+1:]

    new_exchange_rates_df.to_csv('temp_files/newData.csv', index= False)
    
    return new_exchange_rates_df

def mergeOldAndNew_saveToCSV(cloudDf, new_exchange_rates_df: pandas.DataFrame):
    """Takes the two dataframes (one containing the data backed up to the cloud and the other with the new data), combines it, and saves to a .csv
    """
    if not isinstance(cloudDf, pandas.DataFrame):
        cloudAndNewMerge = new_exchange_rates_df
    else:
        cloudAndNewMerge = pandas.concat([cloudDf, new_exchange_rates_df])

    filepath = 'temp_files/' + configuration.Basefile_name + '.csv'
    cloudAndNewMerge.to_csv(filepath, index=False)