import argparse
import pandas as pd
from datetime import datetime, date
from pymongo import MongoClient
from azure.cosmos import CosmosClient, PartitionKey
import json
from jsonschema import validate, ValidationError
import requests
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

# Function to convert non-serializable objects to a serializable form
def convert_to_serializable(obj):
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    elif isinstance(obj, Timestamp):
        return obj.to_pydatetime().isoformat()
    raise TypeError(f"Object of type {type(obj)} is not JSON serializable")

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

# Function to format datetime string or object
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

# Function to process traffic data from GitHub
def process_traffic_data(data):
    if not data or 'views' not in data:
        return pd.DataFrame()  # Return an empty DataFrame if data is invalid or missing

    processed_data = [{
        "Date": format_datetime(item['timestamp'][:10]),
        "Views": item['count'],
        "Unique Visitors": item['uniques']
    } for item in data['views']]

    return pd.DataFrame(processed_data)

# Function to process clones data from GitHub
def process_clones_data(data):
    if not data or 'clones' not in data:
        return pd.DataFrame()  # Return an empty DataFrame if data is invalid or missing

    processed_data = [{
        "Date": format_datetime(item['timestamp'][:10]),
        "Clones": item['count'],
        "Unique Cloners": item['uniques']
    } for item in data['clones']]

    return pd.DataFrame(processed_data)

# Function to process referrers data from GitHub
def process_referrers_data(data):
    if not data:
        return pd.DataFrame()  # Return an empty DataFrame if data is invalid or missing

    processed_data = [{
        "Referrer": item['referrer'],
        "Views": item['count'],
        "Unique Visitors": item['uniques'],
        "Date": format_datetime(datetime.now(), with_time=False)
    } for item in data]

    return pd.DataFrame(processed_data)

# Function to process popular content data from GitHub
def process_popular_content_data(data):
    if not data:
        return pd.DataFrame()  # Return an empty DataFrame if data is invalid or missing

    processed_data = [{
        "Path": item.get('path', 'Unknown'),
        "Title": item.get('title', 'Unknown Title'),
        "Views": item['count'],
        "Unique Visitors": item['uniques'],
        "Date": format_datetime(datetime.now(), with_time=False)
    } for item in data]

    return pd.DataFrame(processed_data)

# Function to process a DataFrame by appending monthly and yearly totals
def process_dataframe(df, date_column, historical_data=False):
    # Remove 'id' column if present
    if 'id' in df.columns:
        df = df.drop('id', axis=1)

    if not historical_data:
        df = append_monthly_yearly_totals(df, date_column)

    return df

# Function to append monthly and yearly totals to a DataFrame
def append_monthly_yearly_totals(df, date_column):
    if date_column in df.columns:
        df[date_column] = pd.to_datetime(df[date_column], errors='coerce')

        # Ensure only numeric columns are included in aggregation
        numeric_cols = df.select_dtypes(include=['number']).columns
        aggregation = {col: 'sum' for col in numeric_cols}

        # Calculate monthly totals
        monthly_totals = df.resample('M', on=date_column).agg(aggregation)
        monthly_totals[date_column] = monthly_totals.index.strftime('%Y-%m')
        monthly_totals['Type'] = 'Monthly Total'

        # Calculate yearly totals
        yearly_totals = df.resample('Y', on=date_column).agg(aggregation)
        yearly_totals[date_column] = yearly_totals.index.strftime('%Y')
        yearly_totals['Type'] = 'Yearly Total'

        # Combine with the original DataFrame
        df = pd.concat([df, monthly_totals, yearly_totals], sort=False).fillna('')
        df.sort_values(by=[date_column, 'Type'], inplace=True, ignore_index=True)

    return df

# Function to clean and format a DataFrame
def clean_dataframe(df, date_column='Date'):
    # Remove '_id' column if present
    if '_id' in df.columns:
        df.drop('_id', axis=1, inplace=True)

    # Convert date_column to datetime, sort the DataFrame, and reset the index
    if date_column in df.columns and not df[date_column].empty:
        df[date_column] = pd.to_datetime(df[date_column], errors='coerce')
        df.sort_values(by=date_column, inplace=True)
        df.reset_index(drop=True, inplace=True)

    return df

# Function to create a MongoDB client
def get_mongo_client(connection_string):
    try:
        client = MongoClient(connection_string)
        return client
    except Exception as e:
        print(f"Error creating MongoDB client: {e}")
        return None

# Function to upsert data to MongoDB
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

# Function to save multiple dataframes to an Excel file
def save_dataframes_to_excel(filename, **dataframes):
    with pd.ExcelWriter(filename, engine='openpyxl') as writer:
        for sheet_name, df in dataframes.items():
            # Check if the df is a DataFrame and not empty
            if isinstance(df, pd.DataFrame) and not df.empty:
                # Clean the DataFrame
                df = clean_dataframe(df)

                # Handle 'Date' formatting
                if 'Date' in df.columns:
                    df['Date'] = df['Date'].apply(lambda x: x.strftime('%Y-%m-%d') if pd.notnull(x) else None)

                # Append monthly and yearly totals for specific sheets
                if sheet_name in ['Referring Sites', 'Popular Content', 'Traffic Stats', 'Git Clones']:
                    df = append_monthly_yearly_totals(df, 'Date')

                # Drop unnecessary columns
                df.drop(columns=['FetchedAt', 'id'], errors='ignore', inplace=True)

                # Write the DataFrame to Excel
                df.to_excel(writer, sheet_name=sheet_name, index=False)
            else:
                print(f"Sheet '{sheet_name}' is empty or not a DataFrame and will not be written to Excel.")

# Function to read a GitHub Personal Access Token from a file
def read_token_from_file(file_path):
    try:
        with open(file_path, 'r') as file:
            return file.read().strip()
    except IOError as e:
        print(f"Error reading token file: {e}")
        return None

# Function to fetch historical data from MongoDB
def fetch_historical_data(client, database_name, collection_name, start_date, end_date):
    db = client[database_name]
    collection = db[collection_name]

    query = {
        'Date': {
            '$gte': start_date,
            '$lte': end_date
        }
    }

    cursor = collection.find(query)
    historical_data = [item for item in cursor]

    return pd.DataFrame(historical_data)

# Main function of the script
def main():
    parser = argparse.ArgumentParser(description='GitHub Repository Traffic Data Fetcher')
    parser.add_argument('--repo', required=True, help='Repository name')
    parser.add_argument('--owner', required=True, help='Organization/user name that owns the repository')
    parser.add_argument('--filename', help='Optional: Specify a filename for the Excel output. If not provided, defaults to {owner}-{repo}-traffic-data.xlsx')
    parser.add_argument('--token-file', required=True, help='Path to a text file containing the GitHub Personal Access Token')
    parser.add_argument('--mongodb-connection-string', help='Optional: MongoDB connection string to store the data')
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

    # Validate and process the fetched data
    if validate_json_data(views_data, traffic_data_schema) and \
       validate_json_data(clones_data, clones_data_schema) and \
       validate_json_data(referrers_data, referrers_data_schema) and \
       validate_json_data(popular_content_data, popular_content_data_schema):

        processed_traffic_data = process_traffic_data(views_data)
        processed_clones_data = process_clones_data(clones_data)
        processed_referrers_data = process_referrers_data(referrers_data)
        processed_popular_content_data = process_popular_content_data(popular_content_data)

        # MongoDB operations
        mongo_client = None
        if args.mongodb_connection_string:
            mongo_client = get_mongo_client(args.mongodb_connection_string)

        if mongo_client:
            upsert_to_mongodb(mongo_client, database_name, "TrafficStats", processed_traffic_data)
            upsert_to_mongodb(mongo_client, database_name, "GitClones", processed_clones_data)
            upsert_to_mongodb(mongo_client, database_name, "ReferringSites", processed_referrers_data)
            upsert_to_mongodb(mongo_client, database_name, "PopularContent", processed_popular_content_data)

        # Prepare dataframes for Excel export
        dataframes_to_save = {
            'Traffic Stats': processed_traffic_data,
            'Git Clones': processed_clones_data,
            'Referring Sites': processed_referrers_data,
            'Popular Content': processed_popular_content_data
        }

        # Save data to Excel
        if dataframes_to_save:
            save_dataframes_to_excel(filename, **dataframes_to_save)
            print(f"Data successfully saved to {filename}")
        else:
            print("No data available to save to Excel.")
    else:
        print("Data validation failed.")

if __name__ == "__main__":
    main()