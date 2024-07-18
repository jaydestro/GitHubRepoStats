import pandas as pd
from pymongo import MongoClient
from azure.cosmos import CosmosClient, PartitionKey, exceptions
import logging
from datetime import datetime
import json
import uuid

logger = logging.getLogger(__name__)

PARTITION_KEY = '/id'  # Define a consistent partition key

def get_mongo_client(connection_string):
    logger.info("Creating MongoDB client")
    client = MongoClient(connection_string)
    logger.info("MongoDB client created successfully")
    return client

def get_cosmos_client(connection_string):
    logger.info("Creating Cosmos DB client")
    client = CosmosClient.from_connection_string(connection_string)
    logger.info("Cosmos DB client created successfully")
    return client

def fetch_all_data_from_mongodb(client, database_name, collection_name):
    logger.info(f"Fetching all data from MongoDB database: {database_name}, collection: {collection_name}")
    db = client[database_name]
    collection = db[collection_name]
    data = list(collection.find({}, {'_id': 0}))  # Exclude the Mongo ID
    logger.info(f"Fetched {len(data)} records from MongoDB")
    return pd.DataFrame(data)

def fetch_all_data_from_cosmosdb(client, database_name, container_name):
    logger.info(f"Fetching all data from Cosmos DB database: {database_name}, container: {container_name}")
    try:
        database = client.get_database_client(database_name)
        container = database.get_container_client(container_name)
        query = "SELECT * FROM c"
        items = list(container.query_items(query, enable_cross_partition_query=True))
        logger.info(f"Fetched {len(items)} records from Cosmos DB")
        return pd.DataFrame(items)
    except exceptions.CosmosResourceNotFoundError:
        logger.error(f"Database or container {database_name}/{container_name} does not exist.")
        return pd.DataFrame()

def append_new_data(client, database_name, collection_name, new_data, date_column, db_type="mongodb"):
    logger.info(f"Appending new data to {db_type} database: {database_name}, collection: {collection_name}")
    if db_type == "mongodb":
        old_df = fetch_all_data_from_mongodb(client, database_name, collection_name)
    elif db_type == "cosmosdb":
        old_df = fetch_all_data_from_cosmosdb(client, database_name, collection_name)
    else:
        raise ValueError(f"Unsupported database type: {db_type}")

    new_df = pd.DataFrame(new_data)

    if date_column not in old_df.columns:
        old_df[date_column] = pd.NaT
    if date_column not in new_df.columns:
        new_df[date_column] = pd.NaT
    new_df[date_column] = pd.to_datetime(new_df[date_column], errors='coerce')
    old_df[date_column] = pd.to_datetime(old_df[date_column], errors='coerce')

    new_df = new_df.dropna(subset=[date_column])
    old_df = old_df.dropna(subset=[date_column])
    combined_df = pd.concat([old_df, new_df], ignore_index=True)
    combined_df.drop_duplicates(subset=[date_column], keep='last', inplace=True)
    combined_df.sort_values(by=date_column, inplace=True)

    logger.info(f"Appended {len(new_df)} new records to {db_type} database")
    return combined_df

def create_cosmos_database_if_not_exists(client, database_name):
    logger.info(f"Creating Cosmos DB database if not exists: {database_name}")
    try:
        database = client.create_database_if_not_exists(id=database_name)
        logger.info(f"Database {database_name} exists or was created successfully")
        return database
    except exceptions.CosmosHttpResponseError as e:
        logger.error(f"Failed to create database: {e}")
        raise

def create_cosmos_container_if_not_exists(client, database_name, container_name):
    logger.info(f"Checking if Cosmos DB container exists: {container_name} in database: {database_name}")
    try:
        database = create_cosmos_database_if_not_exists(client, database_name)
        container = database.get_container_client(container_name)
        container.read()
        logger.info(f"Container {container_name} already exists")
        return container
    except exceptions.CosmosResourceNotFoundError:
        logger.info(f"Creating Cosmos DB container: {container_name} in database: {database_name}")
        try:
            container = database.create_container(
                id=container_name,
                partition_key=PartitionKey(path=PARTITION_KEY),
                offer_throughput=400
            )
            logger.info(f"Container {container_name} created successfully")
            return container
        except exceptions.CosmosHttpResponseError as e:
            logger.error(f"Failed to create container: {e}")
            raise

def save_to_mongodb(client, database_name, collection_name, data):
    logger.info(f"Saving data to MongoDB database: {database_name}, collection: {collection_name}")
    db = client[database_name]
    collection = db[collection_name]

    unique_field_map = {
        'TrafficStats': 'Date',
        'GitClones': 'Date',
        'ReferringSites': 'Referring site',
        'PopularContent': 'Path',
        'Stars': 'Date',
        'Forks': 'Date'
    }

    if collection_name not in unique_field_map:
        raise ValueError(f"Unknown collection name: {collection_name}")

    unique_field = unique_field_map[collection_name]
    for item in data:
        query = {unique_field: item[unique_field]}
        update = {"$set": item}
        collection.update_one(query, update, upsert=True)

    logger.info(f"Saved {len(data)} records to MongoDB collection: {collection_name}")

def validate_json(data):
    """Validate if data is JSON serializable"""
    try:
        json.dumps(data)
        return True
    except (TypeError, OverflowError) as e:
        logger.error(f"Data is not JSON serializable: {e}")
        return False

def convert_to_json_serializable(data):
    """Convert data to JSON serializable format."""
    if isinstance(data, pd.Timestamp):
        return data.isoformat()
    elif isinstance(data, datetime):
        return data.isoformat()
    elif isinstance(data, float) and data.is_integer():
        return int(data)
    elif isinstance(data, float):
        return str(data)
    elif isinstance(data, dict):
        return {key: convert_to_json_serializable(value) for key, value in data.items()}
    elif isinstance(data, list):
        return [convert_to_json_serializable(item) for item in data]
    else:
        return data

def preprocess_data(data):
    """Preprocess data to ensure it matches the expected schema."""
    for item in data:
        if 'Date' in item and isinstance(item['Date'], (pd.Timestamp, datetime)):
            item['Date'] = item['Date'].isoformat()
        if 'Views' in item and isinstance(item['Views'], float):
            item['Views'] = int(item['Views'])
        if 'Unique visitors' in item and isinstance(item['Unique visitors'], float):
            item['Unique visitors'] = int(item['Unique visitors'])
        if 'Clones' in item and isinstance(item['Clones'], float):
            item['Clones'] = int(item['Clones'])
        if 'Unique cloners' in item and isinstance(item['Unique cloners'], float):
            item['Unique cloners'] = int(item['Unique cloners'])
        if 'FetchedAt' in item and isinstance(item['FetchedAt'], (pd.Timestamp, datetime)):
            item['FetchedAt'] = item['FetchedAt'].isoformat()
        if 'id' not in item or pd.isna(item['id']) or item['id'] == "nan":
            item['id'] = str(uuid.uuid4())  # Generate a unique ID
    return data

def validate_and_convert_data(data, schema_validator):
    """Validate and convert data to JSON serializable format."""
    data = preprocess_data(data)
    if not schema_validator(data):
        raise ValueError(f"Invalid data schema: {data}")
    return [convert_to_json_serializable(item) for item in data]

def validate_traffic_stats_schema(data):
    """Validate the schema of TrafficStats data."""
    required_fields = ['Date', 'Views', 'Unique visitors', 'Repo Owner and Name']
    for item in data:
        for field in required_fields:
            if field not in item:
                logger.error(f"Missing field '{field}' in item: {item}")
                return False
            if field == 'Date' and not isinstance(item[field], str):
                logger.error(f"Invalid type for 'Date' in item: {item}")
                return False
            if field == 'Views' and not isinstance(item[field], int):
                logger.error(f"Invalid type for 'Views' in item: {item}")
                return False
            if field == 'Unique visitors' and not isinstance(item[field], int):
                logger.error(f"Invalid type for 'Unique visitors' in item: {item}")
                return False
            if field == 'Repo Owner and Name' and not isinstance(item[field], str):
                logger.error(f"Invalid type for 'Repo Owner and Name' in item: {item}")
                return False
    return True

def validate_clones_schema(data):
    """Validate the schema of Clones data."""
    required_fields = ['Date', 'Clones', 'Unique cloners', 'Repo Owner and Name']
    for item in data:
        for field in required_fields:
            if field not in item:
                logger.error(f"Missing field '{field}' in item: {item}")
                return False
            if field == 'Date' and not isinstance(item[field], str):
                logger.error(f"Invalid type for 'Date' in item: {item}")
                return False
            if field == 'Clones' and not isinstance(item[field], int):
                logger.error(f"Invalid type for 'Clones' in item: {item}")
                return False
            if field == 'Unique cloners' and not isinstance(item[field], int):
                logger.error(f"Invalid type for 'Unique cloners' in item: {item}")
                return False
            if field == 'Repo Owner and Name' and not isinstance(item[field], str):
                logger.error(f"Invalid type for 'Repo Owner and Name' in item: {item}")
                return False
    return True

def validate_referring_sites_schema(data):
    """Validate the schema of ReferringSites data."""
    required_fields = ['Referring site', 'Views', 'Unique visitors', 'FetchedAt', 'Repo Owner and Name']
    for item in data:
        for field in required_fields:
            if field not in item:
                logger.error(f"Missing field '{field}' in item: {item}")
                return False
            if field == 'Referring site' and not isinstance(item[field], str):
                logger.error(f"Invalid type for 'Referring site' in item: {item}")
                return False
            if field == 'Views' and not isinstance(item[field], int):
                logger.error(f"Invalid type for 'Views' in item: {item}")
                return False
            if field == 'Unique visitors' and not isinstance(item[field], int):
                logger.error(f"Invalid type for 'Unique visitors' in item: {item}")
                return False
            if field == 'FetchedAt' and not isinstance(item[field], str):
                logger.error(f"Invalid type for 'FetchedAt' in item: {item}")
                return False
            if field == 'Repo Owner and Name' and not isinstance(item[field], str):
                logger.error(f"Invalid type for 'Repo Owner and Name' in item: {item}")
                return False
    return True

def validate_popular_content_schema(data):
    """Validate the schema of PopularContent data."""
    required_fields = ['Path', 'Views', 'Unique visitors', 'FetchedAt', 'Repo Owner and Name']
    for item in data:
        for field in required_fields:
            if field not in item:
                logger.error(f"Missing field '{field}' in item: {item}")
                return False
            if field == 'Path' and not isinstance(item[field], str):
                logger.error(f"Invalid type for 'Path' in item: {item}")
                return False
            if field == 'Views' and not isinstance(item[field], int):
                logger.error(f"Invalid type for 'Views' in item: {item}")
                return False
            if field == 'Unique visitors' and not isinstance(item[field], int):
                logger.error(f"Invalid type for 'Unique visitors' in item: {item}")
                return False
            if field == 'FetchedAt' and not isinstance(item[field], str):
                logger.error(f"Invalid type for 'FetchedAt' in item: {item}")
                return False
            if field == 'Repo Owner and Name' and not isinstance(item[field], str):
                logger.error(f"Invalid type for 'Repo Owner and Name' in item: {item}")
                return False
    return True

def validate_stars_schema(data):
    """Validate the schema of Stars data."""
    required_fields = ['Date', 'Total Stars', 'Repo Owner and Name']
    for item in data:
        for field in required_fields:
            if field not in item:
                logger.error(f"Missing field '{field}' in item: {item}")
                return False
            if field == 'Date' and not isinstance(item[field], str):
                logger.error(f"Invalid type for 'Date' in item: {item}")
                return False
            if field == 'Total Stars' and not isinstance(item[field], int):
                logger.error(f"Invalid type for 'Total Stars' in item: {item}")
                return False
            if field == 'Repo Owner and Name' and not isinstance(item[field], str):
                logger.error(f"Invalid type for 'Repo Owner and Name' in item: {item}")
                return False
    return True

def validate_forks_schema(data):
    """Validate the schema of Forks data."""
    required_fields = ['Date', 'Total Forks', 'Repo Owner and Name']
    for item in data:
        for field in required_fields:
            if field not in item:
                logger.error(f"Missing field '{field}' in item: {item}")
                return False
            if field == 'Date' and not isinstance(item[field], str):
                logger.error(f"Invalid type for 'Date' in item: {item}")
                return False
            if field == 'Total Forks' and not isinstance(item[field], int):
                logger.error(f"Invalid type for 'Total Forks' in item: {item}")
                return False
            if field == 'Repo Owner and Name' and not isinstance(item[field], str):
                logger.error(f"Invalid type for 'Repo Owner and Name' in item: {item}")
                return False
    return True

def save_to_cosmosdb(client, database_name, container_name, data):
    logger.info(f"Saving data to Cosmos DB database: {database_name}, container: {container_name}")
    schema_validators = {
        'TrafficStats': validate_traffic_stats_schema,
        'GitClones': validate_clones_schema,
        'ReferringSites': validate_referring_sites_schema,
        'PopularContent': validate_popular_content_schema,
        'Stars': validate_stars_schema,
        'Forks': validate_forks_schema
    }

    if container_name not in schema_validators:
        raise ValueError(f"Unknown container name: {container_name}")

    schema_validator = schema_validators[container_name]

    try:
        container = create_cosmos_container_if_not_exists(client, database_name, container_name)
        data = validate_and_convert_data(data, schema_validator)
        for item in data:
            if validate_json(item):
                logger.info(f"Saving item to Cosmos DB: {json.dumps(item)}")
                container.upsert_item(item)
            else:
                logger.error(f"Invalid JSON data: {item}")
        logger.info(f"Saved {len(data)} records to Cosmos DB container: {container_name}")
    except exceptions.CosmosHttpResponseError as e:
        logger.error(f"Error accessing or creating Cosmos DB container: {e}")
        raise
    except ValueError as ve:
        logger.error(f"Invalid JSON data: {ve}")
        raise

def save_data(client, database_name, collection_name, data, db_type="mongodb"):
    logger.info(f"Saving data to {db_type} database: {database_name}, collection/container: {collection_name}")
    if db_type == "mongodb":
        save_to_mongodb(client, database_name, collection_name, data)
    elif db_type == "cosmosdb":
        save_to_cosmosdb(client, database_name, collection_name, data)
    else:
        raise ValueError(f"Unsupported database type: {db_type}")
