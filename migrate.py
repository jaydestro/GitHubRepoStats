import pymongo
from pymongo import MongoClient
from azure.cosmos import CosmosClient, PartitionKey, exceptions
import logging
from tqdm import tqdm  # For progress bar display
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
import datetime
import json

# Configure logging
log_file = 'migration_errors.log'
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
error_log = logging.FileHandler(log_file)
error_log.setLevel(logging.ERROR)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
error_log.setFormatter(formatter)
logging.getLogger().addHandler(error_log)

# MongoDB connection
mongo_uri = "MONGO CONNECTION STRING"
mongo_client = MongoClient(mongo_uri)

# Cosmos DB connection
cosmos_uri = "COSMOS CONNECTION STRING"
cosmos_client = CosmosClient.from_connection_string(cosmos_uri)

# Function to convert datetime objects to ISO format
def convert_datetime_to_str(document):
    """Converts any datetime or date objects in the document to ISO formatted strings."""
    for key, value in document.items():
        if isinstance(value, dict):
            convert_datetime_to_str(value)  # Recursively convert if value is a nested dict
        elif isinstance(value, list):
            for item in value:
                if isinstance(item, dict):
                    convert_datetime_to_str(item)  # Recursively convert if item is a dict
        elif isinstance(value, (datetime.datetime, datetime.date)):
            document[key] = value.isoformat()  # Convert datetime/date to string
        elif value is None:
            document[key] = None  # Handle None values explicitly

# Function to validate JSON
def validate_json(document):
    """Attempts to serialize and deserialize the document as JSON to ensure it is valid JSON."""
    try:
        json_str = json.dumps(document)  # Convert document to JSON string
        json.loads(json_str)  # Deserialize it back to ensure validity
    except (TypeError, ValueError) as e:
        logging.error(f"Invalid JSON document: {document}")
        raise  # Raise an exception if the document is not valid

# Function to migrate a single collection
def migrate_collection(mongo_db, cosmos_database, collection_name):
    """Migrates a collection from MongoDB to Cosmos DB."""
    try:
        logging.info(f"Processing collection: {collection_name}")
        mongo_collection = mongo_db[collection_name]

        # Create corresponding container in Cosmos DB database (if it doesn't exist)
        cosmos_container = cosmos_database.create_container_if_not_exists(
            id=collection_name,
            partition_key=PartitionKey(path="/id")  # Assume documents are partitioned by 'id'
        )

        # Migrate documents in batches of 100
        batch_size = 100
        documents = list(mongo_collection.find())  # Retrieve all documents from MongoDB collection
        for i in range(0, len(documents), batch_size):
            batch = documents[i:i + batch_size]
            for document in batch:
                # Prepare each document for Cosmos DB
                document["id"] = str(document["_id"])  # Convert ObjectId to string and set it as 'id'
                del document["_id"]  # Remove the MongoDB '_id' field

                # Convert datetime objects in the document to strings
                convert_datetime_to_str(document)

                # Validate the document to ensure it is valid JSON
                try:
                    validate_json(document)
                except Exception as e:
                    logging.error(f"Skipping invalid document in collection {collection_name}: {e}")
                    continue  # Skip invalid documents

                # Check if the document already exists in Cosmos DB
                try:
                    cosmos_container.read_item(item=document["id"], partition_key=document["id"])
                    logging.info(f"Document with id {document['id']} already exists in Cosmos DB. Skipping.")
                    continue  # If the document exists, skip it
                except exceptions.CosmosResourceNotFoundError:
                    pass  # Document doesn't exist, proceed to insert it

                # Retry mechanism for upserting documents to Cosmos DB
                retry_count = 3
                for attempt in range(retry_count):
                    try:
                        cosmos_container.upsert_item(document)  # Insert or update document
                        break  # Success, break out of retry loop
                    except exceptions.CosmosHttpResponseError as e:
                        logging.error(f"Error upserting document to Cosmos DB in collection {collection_name}: {e} - Document: {document}")
                        if attempt < retry_count - 1:
                            time.sleep(2 ** attempt)  # Exponential backoff for retries
                        else:
                            raise  # Raise exception if all retries fail

        logging.info(f"Successfully migrated collection: {collection_name}")

    except exceptions.CosmosHttpResponseError as e:
        logging.error(f"An error occurred with Cosmos DB in collection {collection_name}: {e}")
    except Exception as e:
        logging.error(f"An unexpected error occurred in collection {collection_name}: {e}")

# Function to migrate collections and documents from MongoDB to Cosmos DB
def migrate_database(mongo_client, cosmos_client, db_name):
    """Migrates an entire database from MongoDB to Cosmos DB."""
    try:
        logging.info(f"Processing database: {db_name}")
        mongo_db = mongo_client[db_name]

        # Create corresponding database in Cosmos DB (if it doesn't exist)
        cosmos_database = cosmos_client.create_database_if_not_exists(id=db_name)

        # Get all collections from MongoDB database
        collections = mongo_db.list_collection_names()

        # Use ThreadPoolExecutor to migrate collections in parallel
        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = {executor.submit(migrate_collection, mongo_db, cosmos_database, collection_name): collection_name for collection_name in collections}

            # Display progress bar while migrating collections
            for future in tqdm(as_completed(futures), total=len(futures), desc=f"Migrating {db_name}"):
                collection_name = futures[future]
                try:
                    future.result()  # Get the result of the migration, raise if any exception occurred
                except Exception as e:
                    logging.error(f"Error migrating collection {collection_name} from database {db_name}: {str(e)}")

    except exceptions.CosmosHttpResponseError as e:
        logging.error(f"An error occurred with Cosmos DB in database {db_name}: {e}")
    except Exception as e:
        logging.error(f"An unexpected error occurred in database {db_name}: {e}")

# Function to start the migration process for all databases
def migrate_all_databases(mongo_client, cosmos_client):
    """Migrates all databases from MongoDB to Cosmos DB."""
    # Get all database names from MongoDB
    mongo_databases = mongo_client.list_database_names()

    # Filter out internal MongoDB databases and specific ones (e.g., "admin", "local", "config")
    skip_databases = ["admin", "local", "config", "DCComics", "dccomics"]
    mongo_databases = [db for db in mongo_databases if db not in skip_databases]

    # Use ThreadPoolExecutor to migrate databases in parallel
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = {executor.submit(migrate_database, mongo_client, cosmos_client, db_name): db_name for db_name in mongo_databases}

        # Display progress bar while migrating databases
        for future in tqdm(as_completed(futures), total=len(futures), desc="Migrating all databases"):
            db_name = futures[future]
            try:
                future.result()  # Get the result of the migration, raise if any exception occurred
            except Exception as e:
                logging.error(f"Error migrating database {db_name}: {str(e)}")

# Start the migration process
migrate_all_databases(mongo_client, cosmos_client)

# Close MongoDB client after migration
mongo_client.close()
logging.info("Migration completed successfully.")
