import os
import pandas as pd
from pymongo import MongoClient
import logging

logger = logging.getLogger(__name__)

def get_mongo_client(connection_string):
    """Creates and returns a MongoDB client using the provided connection string."""
    return MongoClient(connection_string)

def fetch_all_data_from_mongodb(client, database_name, collection_name):
    db = client[database_name]
    collection = db[collection_name]
    data = list(collection.find({}, {'_id': 0}))  # Exclude the Mongo ID
    return pd.DataFrame(data)

def append_new_data(mongo_client, database_name, collection_name, new_data, date_column):
    old_df = fetch_all_data_from_mongodb(mongo_client, database_name, collection_name)
    new_df = pd.DataFrame(new_data)

    # Ensure date_column exists and is in datetime format
    if date_column not in old_df.columns:
        old_df[date_column] = pd.NaT
    if date_column not in new_df.columns:
        new_df[date_column] = pd.NaT
    new_df[date_column] = pd.to_datetime(new_df[date_column], errors='coerce')
    old_df[date_column] = pd.to_datetime(old_df[date_column], errors='coerce')

    # Clean and merge dataframes
    new_df = new_df.dropna(subset=[date_column])
    old_df = old_df.dropna(subset=[date_column])
    combined_df = pd.concat([old_df, new_df], ignore_index=True)
    combined_df.drop_duplicates(subset=[date_column], keep='last', inplace=True)
    combined_df.sort_values(by=date_column, inplace=True)

    return combined_df

def save_to_mongodb(client, database_name, collection_name, data):
    db = client[database_name]
    collection = db[collection_name]
    if collection_name in ['TrafficStats', 'GitClones', 'Stars', 'Forks']:
        unique_field = 'Date'
    elif collection_name == 'ReferringSites':
        unique_field = 'Referring site'
    elif collection_name == 'PopularContent':
        unique_field = 'Path'
    else:
        raise ValueError(f"Unknown collection name: {collection_name}")
    for item in data:
        query = {unique_field: item[unique_field]}
        update = {"$set": item}
        collection.update_one(query, update, upsert=True)
