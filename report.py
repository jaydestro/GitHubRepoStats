import argparse
import pandas as pd
from datetime import datetime, date, timezone
from pymongo import MongoClient
from azure.cosmos import CosmosClient, PartitionKey
import json
from jsonschema import validate, ValidationError
import requests
from pandas.tseries.offsets import MonthEnd, YearEnd
from pandas._libs.tslibs.timestamps import Timestamp
import numpy as np

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
            "title": {"type": "string"},
            "count": {"type": "integer"},
            "uniques": {"type": "integer"},
        },
        "required": ["path", "title", "count", "uniques"],
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

def format_datetime(timestamp, with_timezone=False, with_time=True):
    if isinstance(timestamp, datetime):
        timestamp = timestamp.strftime('%Y-%m-%d')
    if with_timezone:
        date_obj = datetime.strptime(timestamp, '%Y-%m-%d').date()
    else:
        date_obj = datetime.strptime(timestamp, '%Y-%m-%d').date()

    if with_time:
        return date_obj.isoformat()
    else:
        return date_obj.strftime('%m-%d-%Y')

def process_traffic_data(data):
    if not data or 'views' not in data:
        return []  # Return an empty list if data is invalid or missing
    return [{
        "id": item['timestamp'],
        "Date": format_datetime(item['timestamp'][:10]),
        "Views": item['count'],
        "Unique visitors": item['uniques']
    } for item in data['views']]

def process_clones_data(data):
    if not data or 'clones' not in data:
        return pd.DataFrame()
    return pd.DataFrame([{
        "id": item['timestamp'],
        "Date": format_datetime(item['timestamp'][:10]),
        "Clones": item['count'],
        "Unique cloners": item['uniques']
    } for item in data['clones']])

def process_referrers_data(data):
    if not data:
        return pd.DataFrame()

    referrer_list = []

    for item in data:
        referrer = item['referrer']
        count = item['count']
        uniques = item['uniques']

        referrer_list.append({
            "Referring Site": referrer,
            "Views": count,
            "Unique Visitors": uniques,
            "Date": format_datetime(datetime.now(), with_time=False)
        })

    df = pd.DataFrame(referrer_list)
    df['Month-Year'] = pd.to_datetime(df['Date']).dt.to_period('M')
    return df

def process_popular_content_data(data):
    if not data:
        return pd.DataFrame()

    content_list = []

    for item in data:
        path = item.get('path', 'Unknown')
        title = item.get('title', 'Unknown Title')
        count = item['count']
        uniques = item['uniques']

        content_list.append({
            "Content Path": path,
            "Content Title": title,
            "Views": count,
            "Unique Visitors": uniques,
            "Date": format_datetime(datetime.now(), with_time=False)
        })

    df = pd.DataFrame(content_list)
    df['Month-Year'] = pd.to_datetime(df['Date']).dt.to_period('M')
    return df

    
def process_dataframe(df, date_column):
    df = df.drop_duplicates(subset=[date_column])
    if 'id' in df.columns:
        df = df.drop(columns=['id'])
    df = append_monthly_yearly_totals(df, date_column)
    return df

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
    if isinstance(data, pd.DataFrame):
        data = data.to_dict('records')

    for item in data:
        # Convert non-serializable objects (like Period) to strings
        for key, value in item.items():
            if isinstance(value, pd.Period):
                item[key] = str(value)

        item_id = item.get('id', None)
        if item_id is None:
            # Create a unique ID if 'id' is not present
            item_id = hash(json.dumps(item))
        
        collection.update_one({'id': item_id}, {'$set': item}, upsert=True)

def fetch_all_from_mongodb(client, database_name, collection_name, date_column):
    db = client[database_name]
    collection = db[collection_name]
    data = list(collection.find())
    df = pd.DataFrame(data)
    return process_dataframe(df, date_column)

# Cosmos DB utility functions
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
        container = database.get_container_client(container_name)
        query = "SELECT * FROM c"
        items = list(container.query_items(query=query, enable_cross_partition_query=True))
        
        df = pd.DataFrame(items)
        return process_dataframe(df, date_column)
    except Exception as e:
        print(f"Error fetching data from Cosmos DB: {e}")
        return pd.DataFrame()

def append_monthly_yearly_totals(df, date_column):
    # Create a copy of the DataFrame to avoid SettingWithCopyWarning
    df = df.copy()

    # Drop 'id' column if present
    if 'id' in df.columns:
        df = df.drop('id', axis=1)

    df = df.drop_duplicates(subset=[date_column])
    df[date_column] = pd.to_datetime(df[date_column], errors='coerce')
    df.sort_values(by=date_column, inplace=True)

    # Ensure only numeric columns are included in aggregation
    numeric_cols = df.select_dtypes(include=['number']).columns

    # Exclude rows with 'NaT' in the date column
    valid_data_df = df[df[date_column].notna()]

    # Calculate monthly and yearly totals
    monthly_totals = valid_data_df.resample('M', on=date_column)[numeric_cols].sum()
    monthly_totals['Type'] = 'Monthly Total'
    yearly_totals = valid_data_df.resample('Y', on=date_column)[numeric_cols].sum()
    yearly_totals['Type'] = 'Yearly Total'

    # Append totals
    df = pd.concat([df, monthly_totals, yearly_totals], ignore_index=True)
    return df


def clean_dataframe(df, date_column='Date'):
    # Remove '_id' column if present
    if '_id' in df.columns:
        df.drop('_id', axis=1, inplace=True)
    
    # Check if the date_column exists in DataFrame and is not empty
    if date_column in df.columns and not df[date_column].empty:
        # Convert date_column to datetime, sort the DataFrame, and reset the index
        df[date_column] = pd.to_datetime(df[date_column], errors='coerce')
        df.sort_values(by=date_column, inplace=True)
        df.reset_index(drop=True, inplace=True)
    
    return df

def save_dataframes_to_excel(filename, **dataframes):
    with pd.ExcelWriter(filename, engine='openpyxl') as writer:
        for sheet_name, df in dataframes.items():
            # Clean the DataFrame
            df = clean_dataframe(df)

            # Handle 'Date' formatting
            if 'Date' in df.columns:
                df['Date'] = df['Date'].apply(lambda x: x.strftime('%m-%d-%Y') if pd.notnull(x) else None)

            # Append monthly and yearly totals for specific sheets
            if sheet_name in ['Referring Sites', 'Popular Content', 'Traffic Stats', 'Git Clones']:
                df = append_monthly_yearly_totals(df, 'Date')

            # Drop unnecessary columns
            df.drop(columns=['FetchedAt', 'id'], errors='ignore', inplace=True)

            # Write the DataFrame to Excel
            if not df.empty:
                df.to_excel(writer, sheet_name=sheet_name, index=False)

def read_token_from_file(file_path):
    try:
        with open(file_path, 'r') as file:
            return file.read().strip()
    except IOError as e:
        print(f"Error reading token file: {e}")
        return None

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

    views_data = get_github_data(f"{base_url}/traffic/views", token)
    clones_data = get_github_data(f"{base_url}/traffic/clones", token)
    referrers_data = get_github_data(f"{base_url}/traffic/popular/referrers", token)
    popular_content_data = get_github_data(f"{base_url}/traffic/popular/paths", token)

    if not validate_json_data(views_data, traffic_data_schema) or \
       not validate_json_data(clones_data, clones_data_schema) or \
       not validate_json_data(referrers_data, referrers_data_schema) or \
       not validate_json_data(popular_content_data, popular_content_data_schema):
        return

    processed_traffic_data = process_traffic_data(views_data)
    processed_clones_data = process_clones_data(clones_data)
    processed_referrers_data = process_referrers_data(referrers_data)
    processed_popular_content_data = process_popular_content_data(popular_content_data)

    mongo_client = None
    if args.mongodb_connection_string:
        mongo_client = get_mongo_client(args.mongodb_connection_string)

    cosmos_client = None
    if args.cosmos_db_connection_string:
        cosmos_client = get_cosmos_client(args.cosmos_db_connection_string)

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

    try:
        if dataframes_to_save:
            save_dataframes_to_excel(filename, **dataframes_to_save)
            print(f"Data successfully saved to {filename}")
        else:
            print("No data available to save to Excel.")
    except Exception as e:
        print(f"Error saving data to Excel: {e}")

if __name__ == "__main__":
    main()