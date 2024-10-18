import os
from src import logger
from dotenv import load_dotenv
from azure.cosmos.aio import CosmosClient
from azure.cosmos import exceptions, PartitionKey

load_dotenv()

endpoint = os.getenv('endpoint')
key = os.getenv('key')
database_name = os.getenv('database_name')
container_name_cosmos = os.getenv('container_name_cosmos')


async def get_cosmos_client():
    return CosmosClient(endpoint, credential=key)

async def get_or_create_database(client, database_name):
    try:
        database = client.get_database_client(database_name)
        await database.read()
        print(f"Database '{database_name}' already exists")
    except exceptions.CosmosResourceNotFoundError:
        database = await client.create_database(database_name)
        print(f"Database '{database_name}' created")
    return database

async def get_or_create_container(database, container_name):
    try:
        container = database.get_container_client(container_name)
        await container.read()
        print(f"Container '{container_name}' already exists")
    except exceptions.CosmosResourceNotFoundError:
        container = await database.create_container(id=container_name, partition_key=PartitionKey(path="/date"))
        print(f"Container '{container_name}' created")
    return container

# async def write_exchange_rates_to_cosmosdb(exchange_rates_dict):

#     client = await get_cosmos_client()
#     async with client as client: 
#         database = await get_or_create_database(client, database_name)
#         container = await get_or_create_container(database, container_name_cosmos)

#         for rate in exchange_rates_dict:
#             await container.upsert_item(rate)

async def write_exchange_rates_to_cosmosdb(exchange_rates_dict):
    client = await get_cosmos_client()
    async with client as client:
        database = await get_or_create_database(client, database_name)
        container = await get_or_create_container(database, container_name_cosmos)

        for rate in exchange_rates_dict:
            # Log the document being sent
            logger.info("Sending document: %s", rate)
            
            # Ensure the document has a 'date' field if using 'date' as partition key
            if 'date' not in rate:
                logger.error("Document missing 'date' field: %s", rate)
                continue  # Skip documents that don't have the 'date' field

            try:
                await container.upsert_item(rate)
                logger.info("Successfully upserted document with ID: %s", rate['id'])
            except exceptions.CosmosHttpResponseError as e:
                logger.error("Failed to upsert document with ID: %s. Error: %s", rate['id'], e)
                raise
