import argparse
import pandas as pd
from datetime import datetime, date, timezone
from pymongo import MongoClient
from azure.cosmos import CosmosClient, PartitionKey
import json
from jsonschema import validate, ValidationError
import requests
from pandas._libs.tslibs.timestamps import Timestamp

# Schema for traffic data
traffic_data_schema = {
    "type": "object",
    "properties": {
        "views": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "timestamp": {"type": "string"},
                    "count": {"type": "integer"},
                    "uniques": {"type": "integer"},
                },
                "required": ["timestamp", "count", "uniques"],
            },
        },
    },
    "required": ["views"],
}

# Schema for clones data
clones_data_schema = {
    "type": "object",
    "properties": {
        "clones": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "timestamp": {"type": "string"},
                    "count": {"type": "integer"},
                    "uniques": {"type": "integer"},
                },
                "required": ["timestamp", "count", "uniques"],
            },
        },
        "count": {"type": "integer"},
        "uniques": {"type": "integer"},
    },
    "required": ["clones", "count", "uniques"],
}

# Schema for referrers data
referrers_data_schema = {
    "type": "array",
    "items": {
        "type": "object",
        "properties": {
            "referrer": {"type": "string"},
            "count": {"type": "integer"},
            "uniques": {"type": "integer"},
        },
        "required": ["referrer", "count", "uniques"],
    },
}

# Schema for popular content data
popular_content_data_schema = {
    "type": "array",
    "items": {
        "type": "object",
        "properties": {
            "path": {"type": "string"},
            "count": {"type": "integer"},
            "uniques": {"type": "integer"},
        },
        "required": ["path", "count", "uniques"],
    },
}

def convert_to_serializable(obj):
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    elif isinstance(obj, Timestamp):
        return obj.to_pydatetime().isoformat()
    raise TypeError(f"Object of type {type(obj)} is not JSON serializable")

def validate_json_data(data, schema):
    try:
        validate(instance=data, schema=schema)
        return True
    except ValidationError as e:
        print(f"Validation error: {e.message}")
        return False

def get_github_data(api_url, token):
    headers = {'Authorization': f'token {token}'}
    response = requests.get(api_url, headers=headers)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Failed to fetch data: {response.status_code} - {response.text}")
        return None

# Function to format datetime for serialization
def format_datetime(timestamp, with_timezone=False):
    if isinstance(timestamp, datetime):
        timestamp = timestamp.strftime('%Y-%m-%d')
    if with_timezone:
        return datetime.strptime(timestamp, '%Y-%m-%d', tzinfo=timezone.utc).date().isoformat()
    else:
        return datetime.strptime(timestamp, '%Y-%m-%d').date().isoformat()

# Function to process traffic data
def process_traffic_data(data):
    return [{
        "id": item['timestamp'],  # Assuming timestamp can serve as a unique ID
        "Date": format_datetime(item['timestamp'][:10]),
        "Views": item['count'],
        "Unique visitors": item['uniques']
    } for item in data['views']]

# Function to process clones data
def process_clones_data(data):
    return [{
        "id": item['timestamp'],
        "Date": format_datetime(item['timestamp'][:10]),
        "Clones": item['count'],
        "Unique cloners": item['uniques']
    } for item in data['clones']]

# Function to process referrers data
def process_referrers_data(data):
    timestamp = format_datetime(datetime.now())
    return [{
        "id": item['referrer'] + timestamp,  # Create a composite ID
        "Referring site": item['referrer'],
        "Views": item['count'],
        "Unique visitors": item['uniques'],
        "FetchedAt": timestamp
    } for item in data]

# Function to process popular content data
def process_popular_content_data(data):
    timestamp = format_datetime(datetime.now())
    return [{
        "id": item['path'] + timestamp,  # Create a composite ID
        "Path": item['path'],
        "Views": item['count'],
        "Unique visitors": item['uniques'],
        "FetchedAt": timestamp
    } for item in data]

# MongoDB utility functions
def get_mongo_client(connection_string):
    try:
        client = MongoClient(connection_string)
        return client
    except Exception as e:
        print(f"Error creating MongoDB client: {e}")
        return None

def upsert_to_mongodb(client, database_name, collection_name, data):
    db = client[database_name]
    collection = db[collection_name]
    for item in data:
        collection.update_one({'id': item['id']}, {'$set': item}, upsert=True)

def fetch_all_from_mongodb(client, database_name, collection_name, date_column):
    db = client[database_name]
    collection = db[collection_name]
    data = list(collection.find())
    df = pd.DataFrame(data)
    return process_dataframe(df, date_column)


# Function to create a Cosmos DB client
def get_cosmos_client(connection_string):
    try:
        client = CosmosClient.from_connection_string(connection_string)
        return client
    except Exception as e:
        print(f"Error creating Cosmos DB client: {e}")
        return None

def upsert_to_cosmos_db(client, database_name, collection_name, data):
    try:
        database = client.create_database_if_not_exists(id=database_name)
        container = database.create_container_if_not_exists(
            id=collection_name,
            partition_key=PartitionKey(path="/id")
        )
        for item in data:
            item_dict = json.loads(json.dumps(item, default=convert_to_serializable))
            container.upsert_item(item_dict)
    except Exception as e:
        print(f"Error upserting data to Cosmos DB: {e}")

def fetch_all_from_cosmos_db(client, database_name, container_name, date_column):
    try:
        database = client.get_database_client(database=database_name)
        container = database.get_container_client(container=container_name)
        query = "SELECT * FROM c"
        items = list(container.query_items(query=query, enable_cross_partition_query=True))
        
        # Convert to DataFrame
        df = pd.DataFrame(items)

        # Process DataFrame to remove duplicates and drop 'id' column
        df = process_dataframe(df, date_column)

        return df
    except Exception as e:
        print(f"Error fetching data from Cosmos DB: {e}")
        return pd.DataFrame()

def process_dataframe(df, date_column):
    df = df.drop_duplicates(subset=[date_column])
    if 'id' in df.columns:
        df = df.drop(columns=['id'])
    return df

# Function to save multiple DataFrames to an Excel file
def save_dataframes_to_excel(filename, **dataframes):
    with pd.ExcelWriter(filename, engine='openpyxl') as writer:
        for sheet_name, df in dataframes.items():
            df.to_excel(writer, sheet_name=sheet_name, index=False)

# Function to get GitHub Personal Access Token from a file
def read_token_from_file(file_path):
    try:
        with open(file_path, 'r') as file:
            return file.read().strip()
    except IOError as e:
        print(f"Error reading token file: {e}")
        return None


# Main function with command-line interface
def main():
    parser = argparse.ArgumentParser(description='GitHub Repository Traffic Data Fetcher')
    parser.add_argument('--repo', required=True, help='Repository name')
    parser.add_argument('--owner', required=True, help='Organization/user name that owns the repository')
    parser.add_argument('--filename', help='Optional: Specify a filename for the Excel output. If not provided, defaults to {owner}-{repo}-traffic-data.xlsx')
    parser.add_argument('--token-file', required=True, help='Path to a text file containing the GitHub Personal Access Token')
    parser.add_argument('--mongodb-connection-string', help='Optional: MongoDB connection string to store the data')
    parser.add_argument('--cosmos-db-connection-string', help='Optional: Cosmos DB connection string to store the data')
    args = parser.parse_args()

    token = read_token_from_file(args.token_file)
    if not token:
        return

    base_url = f"https://api.github.com/repos/{args.owner}/{args.repo}"
    database_name = args.repo.replace("-", "").capitalize()
    filename = args.filename or f"{args.owner}-{args.repo}-traffic-data.xlsx"

    # Fetch and process data from GitHub API
    views_data = get_github_data(f"{base_url}/traffic/views", token)
    clones_data = get_github_data(f"{base_url}/traffic/clones", token)
    referrers_data = get_github_data(f"{base_url}/traffic/popular/referrers", token)
    popular_content_data = get_github_data(f"{base_url}/traffic/popular/paths", token)

    # Validate and process data
    if not validate_json_data(views_data, traffic_data_schema) or \
       not validate_json_data(clones_data, clones_data_schema) or \
       not validate_json_data(referrers_data, referrers_data_schema) or \
       not validate_json_data(popular_content_data, popular_content_data_schema):
        return

    processed_traffic_data = process_traffic_data(views_data)
    processed_clones_data = process_clones_data(clones_data)
    processed_referrers_data = process_referrers_data(referrers_data)
    processed_popular_content_data = process_popular_content_data(popular_content_data)

    # Check and Initialize MongoDB Client if connection string is provided
    mongo_client = None
    if args.mongodb_connection_string:
        mongo_client = get_mongo_client(args.mongodb_connection_string)

    # Check and Initialize Cosmos DB Client if connection string is provided
    cosmos_client = None
    if args.cosmos_db_connection_string:
        cosmos_client = get_cosmos_client(args.cosmos_db_connection_string)

    # Upsert data into the specified databases
    if mongo_client:
        upsert_to_mongodb(mongo_client, database_name, "TrafficStats", processed_traffic_data)
        upsert_to_mongodb(mongo_client, database_name, "GitClones", processed_clones_data)
        upsert_to_mongodb(mongo_client, database_name, "ReferringSites", processed_referrers_data)
        upsert_to_mongodb(mongo_client, database_name, "PopularContent", processed_popular_content_data)

    if cosmos_client:
        upsert_to_cosmos_db(cosmos_client, database_name, "TrafficStats", processed_traffic_data)
        upsert_to_cosmos_db(cosmos_client, database_name, "GitClones", processed_clones_data)
        upsert_to_cosmos_db(cosmos_client, database_name, "ReferringSites", processed_referrers_data)
        upsert_to_cosmos_db(cosmos_client, database_name, "PopularContent", processed_popular_content_data)

    # Fetch all data from the specified databases and overwrite Excel sheet
    dataframes_to_save = {}
    if mongo_client:
        traffic_df = fetch_all_from_mongodb(mongo_client, database_name, "TrafficStats", "Date")
        clones_df = fetch_all_from_mongodb(mongo_client, database_name, "GitClones", "Date")
        referrers_df = fetch_all_from_mongodb(mongo_client, database_name, "ReferringSites", "FetchedAt")
        popular_content_df = fetch_all_from_mongodb(mongo_client, database_name, "PopularContent", "FetchedAt")
        dataframes_to_save = {
            'Traffic Stats': traffic_df,
            'Git Clones': clones_df,
            'Referring Sites': referrers_df,
            'Popular Content': popular_content_df
        }
    elif cosmos_client:
        traffic_df = fetch_all_from_cosmos_db(cosmos_client, database_name, "TrafficStats", "Date")
        clones_df = fetch_all_from_cosmos_db(cosmos_client, database_name, "GitClones", "Date")
        referrers_df = fetch_all_from_cosmos_db(cosmos_client, database_name, "ReferringSites", "FetchedAt")
        popular_content_df = fetch_all_from_cosmos_db(cosmos_client, database_name, "PopularContent", "FetchedAt")
        dataframes_to_save = {
            'Traffic Stats': traffic_df,
            'Git Clones': clones_df,
            'Referring Sites': referrers_df,
            'Popular Content': popular_content_df
        }

    if dataframes_to_save:
        save_dataframes_to_excel(filename, **dataframes_to_save)
        print(f"Data successfully saved to {filename}")

if __name__ == "__main__":
    main()
