import io
import pandas
from src.connector import blob
from src.configuration import configuration

def downloadExistingCSVFromBlobAndGetDataframe() -> pandas.DataFrame:
    """Downloads the currently cloud-backed-up csv and converts it to a dataframe and returns it.
    """
    df:pandas.DataFrame = None
    try:
        blob_name = configuration.Basefile_name + '.csv'
        current_csv_string = blob.download_from_blob(blob_name)
        df = None
        df = pandas.read_csv(io.StringIO(current_csv_string))
    except Exception as e:
        print(e)
    return df
    

def getNewRowsOnlyAndSaveToCSV(cloudDF: pandas.DataFrame, exchange_rates_df: pandas.DataFrame) -> pandas.DataFrame:
    """Compares the dataframe corresponding to the cloud-backed-up csv, and the fresh dataframe taken from the central bank website, 
    and returns just the new data from the end of the fresh dataframe as a new dataframe.
    NOTE: New data not at the end of the exchange_rates_df will be missed (but this is not expected).
    
    Args:
        cloudDF (pandas.DataFrame): dataframe corresponding to the cloud-backed-up csv
        exchange_rates_df (pandas.DataFrame): dataframe corresponding to the csv freshly downloaded from the central bank website

    Returns:
        pandas.DataFrame: just the new month(s)' data from the exchange_rates_df
    """
    if not isinstance(cloudDF, pandas.DataFrame):
        new_exchange_rates_df = exchange_rates_df
    else:
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
    
    return new_exchange_rates_df

def mergeOldAndNew_saveToCSV(cloudDf: pandas.DataFrame, new_exchange_rates_df: pandas.DataFrame) -> str:
    """Combines the currently-backed-up (old) records, and the new records, and returns it as a csv string.
    NOTE: The reason we don't just backup the freshly downloaded dataframe directly is in case of format changes, or removal of old data.

    Args:
        cloudDf (pandas.DataFrame): dataframe corresponding to the cloud-backed-up csv
        new_exchange_rates_df (pandas.DataFrame): just the new month(s)' data

    Returns:
        str: old + new data as a csv string
    """
    if not isinstance(cloudDf, pandas.DataFrame):
        cloudAndNewMerge = new_exchange_rates_df
    else:
        cloudAndNewMerge = pandas.concat([cloudDf, new_exchange_rates_df])

    # cloudAndNewMergeString = io.StringIO()
    cloudAndNewMergeString = cloudAndNewMerge.to_csv(index=False)
    return cloudAndNewMergeString