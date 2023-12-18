import argparse
import pandas as pd
from datetime import datetime
from pymongo import MongoClient
from azure.cosmos import CosmosClient, PartitionKey
import json
from jsonschema import validate, ValidationError
import requests
from datetime import datetime

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

# Function to validate JSON data against a schema
def validate_json_data(data, schema):
    try:
        validate(instance=data, schema=schema)
        return True
    except ValidationError as e:
        print(f"Validation error: {e.message}")
        return False

# Function to fetch data from GitHub API
def get_github_data(api_url, token):
    headers = {'Authorization': f'token {token}'}
    response = requests.get(api_url, headers=headers)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Failed to fetch data: {response.status_code} - {response.text}")
        return None

# Function to process traffic data (adjusted for JSON serialization)
def process_traffic_data(data):
    return [{
        "Date": datetime.strptime(item['timestamp'][:10], '%Y-%m-%d').date().isoformat(),
        "Views": item['count'], 
        "Unique visitors": item['uniques']
    } for item in data['views']]

# Function to process clones data (adjusted for JSON serialization)
def process_clones_data(data):
    return [{
        "Date": datetime.strptime(item['timestamp'][:10], '%Y-%m-%d').date().isoformat(),
        "Clones": item['count'], 
        "Unique cloners": item['uniques']
    } for item in data['clones']]

# Updated function to process referrers data with timestamp (adjusted for JSON serialization)
def process_referrers_data(data):
    timestamp = datetime.now().isoformat()
    return [{
        "Referring site": item['referrer'], 
        "Views": item['count'], 
        "Unique visitors": item['uniques'],
        "FetchedAt": timestamp
    } for item in data]

# Updated function to process popular content data with timestamp (adjusted for JSON serialization)
def process_popular_content_data(data):
    timestamp = datetime.now().isoformat()
    return [{
        "Path": item['path'], 
        "Views": item['count'], 
        "Unique visitors": item['uniques'],
        "FetchedAt": timestamp
    } for item in data]

# Function to read the Personal Access Token from a file
def read_token_from_file(file_path):
    try:
        with open(file_path, 'r') as file:
            return file.read().strip()
    except IOError as e:
        print(f"Error reading token file: {e}")
        return None

# Function to get the last recorded date from a DataFrame
def get_last_recorded_date(df):
    if df.empty or "Date" not in df.columns:
        return None
    return pd.to_datetime(df["Date"]).max()

# Function to append new data to the DataFrame
def append_new_data(old_df, new_data, date_column):
    new_df = pd.DataFrame(new_data)
    if not old_df.empty:
        last_date = get_last_recorded_date(old_df)
        new_df = new_df[pd.to_datetime(new_df[date_column]) > last_date]
    return pd.concat([old_df, new_df], ignore_index=True)

def add_grouped_totals(df, date_column, metrics_columns):
    if not df.empty:
        # Convert the date_column to datetime type
        df[date_column] = pd.to_datetime(df[date_column])

        # Prepare a DataFrame with only the necessary columns
        metrics_df = df[[date_column] + metrics_columns]

        # Perform grouping operations
        monthly_totals = metrics_df.groupby(pd.Grouper(key=date_column, freq="M"))[metrics_columns].sum().reset_index()
        monthly_totals[date_column] = monthly_totals[date_column].dt.strftime('%Y-%m')
        yearly_totals = metrics_df.groupby(pd.Grouper(key=date_column, freq="Y"))[metrics_columns].sum().reset_index()
        yearly_totals[date_column] = yearly_totals[date_column].dt.strftime('%Y')

        # Concatenate the original dataframe with the grouped totals
        return pd.concat([df, monthly_totals, yearly_totals], ignore_index=True)
    return df

# Function to create a Cosmos DB client
def get_cosmos_client(connection_string):
    try:
        client = CosmosClient.from_connection_string(connection_string)
        return client
    except Exception as e:
        print(f"Error creating Cosmos DB client: {e}")
        return None

# Function to upsert data to Cosmos DB
def save_to_cosmos_db(client, database_name, collection_name, data):
    try:
        # Create or get database
        database = client.create_database_if_not_exists(id=database_name)
        # Create or get container
        container = database.create_container_if_not_exists(
            id=collection_name,
            partition_key=PartitionKey(path="/id")
        )
        # Upsert each item in the data
        for item in data:
            container.upsert_item(item)
    except Exception as e:
        print(f"Error saving data to Cosmos DB: {e}")

def get_mongo_client(connection_string):
    try:
        client = MongoClient(connection_string)
        return client
    except Exception as e:
        print(f"Error creating MongoDB client: {e}")
        return None
    
def save_to_mongodb(client, database_name, collection_name, data):
    try:
        db = client[database_name]
        collection = db[collection_name]
        collection.insert_many(data)
    except Exception as e:
        print(f"Error saving data to MongoDB: {e}")

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

    # Fetch and process data
    views_data = get_github_data(f"{base_url}/traffic/views", token)
    clones_data = get_github_data(f"{base_url}/traffic/clones", token)
    referrers_data = get_github_data(f"{base_url}/traffic/popular/referrers", token)
    popular_content_data = get_github_data(f"{base_url}/traffic/popular/paths", token)

    # Validate fetched data against schemas
    if not validate_json_data(views_data, traffic_data_schema) or \
       not validate_json_data(clones_data, clones_data_schema) or \
       not validate_json_data(referrers_data, referrers_data_schema) or \
       not validate_json_data(popular_content_data, popular_content_data_schema):
        return

    traffic_df = append_new_data(pd.DataFrame(), process_traffic_data(views_data), 'Date')
    clones_df = append_new_data(pd.DataFrame(), process_clones_data(clones_data), 'Date')
    referrers_df = append_new_data(pd.DataFrame(), process_referrers_data(referrers_data), 'Referring site')
    popular_content_df = append_new_data(pd.DataFrame(), process_popular_content_data(popular_content_data), 'Path')

    # Add grouped totals for Traffic Stats and Git Clones
    traffic_df = add_grouped_totals(traffic_df, 'Date', ['Views', 'Unique visitors'])
    clones_df = add_grouped_totals(clones_df, 'Date', ['Clones', 'Unique cloners'])
    # Add grouped totals for Referring Sites and Popular Content with timestamp handling
    referrers_df = add_grouped_totals(referrers_df, 'FetchedAt', ['Views', 'Unique visitors'])
    popular_content_df = add_grouped_totals(popular_content_df, 'FetchedAt', ['Views', 'Unique visitors'])

    # Save to Excel
    with pd.ExcelWriter(filename, engine='openpyxl') as writer:
        traffic_df.to_excel(writer, sheet_name='Traffic Stats', index=False)
        clones_df.to_excel(writer, sheet_name='Git Clones', index=False)
        referrers_df.to_excel(writer, sheet_name='Referring Sites', index=False)
        popular_content_df.to_excel(writer, sheet_name='Popular Content', index=False)

    print(f"Data successfully saved to {filename}")

    # Save to MongoDB if connection string is provided
    if args.mongodb_connection_string:
        mongo_client = get_mongo_client(args.mongodb_connection_string)
        save_to_mongodb(mongo_client, database_name, "TrafficStats", traffic_df.to_dict('records'))
        save_to_mongodb(mongo_client, database_name, "GitClones", clones_df.to_dict('records'))
        save_to_mongodb(mongo_client, database_name, "ReferringSites", referrers_df.to_dict('records'))
        save_to_mongodb(mongo_client, database_name, "PopularContent", popular_content_df.to_dict('records'))

        print("Data also saved to MongoDB")

    # Save to Cosmos DB if connection string is provided
    if args.cosmos_db_connection_string:
        cosmos_client = get_cosmos_client(args.cosmos_db_connection_string)
        save_to_cosmos_db(cosmos_client, database_name, "TrafficStats", traffic_df.to_dict('records'))
        save_to_cosmos_db(cosmos_client, database_name, "GitClones", clones_df.to_dict('records'))
        save_to_cosmos_db(cosmos_client, database_name, "ReferringSites", referrers_df.to_dict('records'))
        save_to_cosmos_db(cosmos_client, database_name, "PopularContent", popular_content_df.to_dict('records'))

        print("Data also saved to Cosmos DB")

if __name__ == "__main__":
    main()