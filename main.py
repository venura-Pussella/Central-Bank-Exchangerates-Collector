import asyncio
import platform
import pandas 
from src import logger
from src.utils.log_utils import send_log
from src.configuration.configuration import webpage_url, css_selector
from src.operators.download_excel_content_to_bytes import download_excel_content,read_excel_to_dataframe
from src.operators.find_excel_link import find_excel_link,fetch_webpage
from src.operators.data_transformer import process_dataframe,creating_headers,clean_column_names,clean_dataframe
from src.configuration import configuration
from src.connector.blob import upload_to_blob
from src.operators import csv_data_converter
from src.connector.cosmos_db import write_exchange_rates_to_cosmosdb
from src.operators.convert_to_cosmosdb import convert_df_to_cosmos_db_format

async def main():

    try:
        # Fetch the webpage and find the Excel file link
        logger.info("Starting Central Bank Exchangerates Extraction Process.")
        soup = fetch_webpage(webpage_url)
        excel_url = find_excel_link(soup, css_selector)
        logger.info("Excel file link found: %s", excel_url)

        # Download the Excel content and load it into a DataFrame
        logger.info("Dataframe creation started")
        excel_content = download_excel_content(excel_url)
        df = read_excel_to_dataframe(excel_content)
        # df = pandas.read_excel('central_bank_exchangerates_file.xlsx') # TESTING
        logger.info("Dataframe created") 

        # Data transformations
        logger.info("Data transformation started")

        df=process_dataframe(df)
        df=creating_headers(df)
        df=clean_column_names(df)
        df=clean_dataframe(df)
        logger.info("Data transformation finished") 

        # compare records with the csv already backed up to the cloud, so that only the new rows of data at the bottom are processed
        cloudDF = csv_data_converter.downloadExistingCSVFromBlobAndGetDataframe()
        logger.info("Cloud csv downloaded") 
        new_exchange_rates_df = csv_data_converter.getNewRowsOnlyAndSaveToCSV(cloudDF, df)
        logger.info("CSV data created.")
        cloudAndNewMergeString = csv_data_converter.mergeOldAndNew_saveToCSV(cloudDF, new_exchange_rates_df)

        # backup the updated record to Azure storage
        upload_to_blob(cloudAndNewMergeString)
        logger.info("Successfully uploaded to blob.")
        
        # Convert to JSON format
        cosmos_db_documents_json = convert_df_to_cosmos_db_format(new_exchange_rates_df)
        
        logger.info("Successfully converted DataFrame to Cosmos DB format.")

        # Upload to Cosmos DB
        await write_exchange_rates_to_cosmosdb(cosmos_db_documents_json)
        logger.info("Completion of data ingestion to Cosmos DB.")

        # send_log on successful completion
        send_log(

            service_type="Azure Function",
            application_name="Central Bank Exchangerates Collector",
            project_name="Dockit Exchange Rates History",
            project_sub_name="Exchangerates History",
            azure_hosting_name="AI Services",
            developmental_language="Python",
            description="Bank Exchange Rates - Function Application",
            created_by="BrownsAIsevice",
            log_print="Successfully completed data ingestion to Cosmos DB.",
            running_within_minutes=43200,
            error_id=0

        )
        logger.info("sent success log to function monitoring service.")

    except Exception as e:
        # send_log on error
        logger.error(f"An error occurred: {e}")

        send_log(

            service_type="Azure Function",
            application_name="Central Bank Exchangerates Collector",
            project_name="Dockit Exchange Rates History",
            project_sub_name="Exchangerates History",
            azure_hosting_name="AI Services",
            developmental_language="Python",
            description="Bank Exchange Rates - Function Application",
            created_by="BrownsAIsevice",
            log_print="An error occurred: " + str(e),
            running_within_minutes=43200,
            error_id=1
        )
        raise

def run_main():

    if platform.system() == "Windows":
        asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
    # asyncio.run(main())
    try:
        loop = asyncio.get_event_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

    # loop = asyncio.get_event_loop()
    loop.run_until_complete(main())

if __name__ == '__main__':
    run_main()