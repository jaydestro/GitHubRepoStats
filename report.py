import requests
import pandas as pd
import argparse
from datetime import datetime
import os
from pymongo import MongoClient

def get_github_data(api_url, token):
    """
    Fetches data from the GitHub API.
    """
    headers = {'Authorization': f'token {token}'}
    response = requests.get(api_url, headers=headers)

    if response.status_code == 200:
        return response.json()
    else:
        print(f"Failed to fetch data: {response.status_code} - {response.text}")
        return None

def process_traffic_data(data):
    """
    Transforms traffic data into the desired format.
    """
    return [{"Date": datetime.strptime(item['timestamp'][:10], '%Y-%m-%d').date(), 
             "Views": item['count'], 
             "Unique visitors": item['uniques']} for item in data['views']]

def process_clones_data(data):
    """
    Transforms clones data into the desired format.
    """
    return [{"Date": datetime.strptime(item['timestamp'][:10], '%Y-%m-%d').date(), 
             "Clones": item['count'], 
             "Unique cloners": item['uniques']} for item in data['clones']]

def process_referrers_data(data):
    """
    Transforms referring sites data into the desired format.
    """
    return [{"Referring site": item['referrer'], 
             "Views": item['count'], 
             "Unique visitors": item['uniques']} for item in data]

def process_popular_content_data(data):
    """
    Transforms popular content data into the desired format.
    """
    return [{"Path": item['path'], 
             "Views": item['count'], 
             "Unique visitors": item['uniques']} for item in data]

def read_token_from_file(file_path):
    """
    Reads the Personal Access Token from a file.
    """
    try:
        with open(file_path, 'r') as file:
            return file.read().strip()
    except IOError as e:
        print(f"Error reading token file: {e}")
        return None

def get_last_recorded_date(df):
    """
    Get the last recorded date from a DataFrame.
    """
    if df.empty or "Date" not in df.columns:
        return None
    return pd.to_datetime(df["Date"]).max()

def append_new_data(old_df, new_data, date_column):
    """
    Append new data to the DataFrame.
    """
    new_df = pd.DataFrame(new_data)
    if not old_df.empty:
        last_date = get_last_recorded_date(old_df)
        new_df = new_df[pd.to_datetime(new_df[date_column]) > last_date]
    return pd.concat([old_df, new_df], ignore_index=True)

def add_grouped_totals(df, date_column, metrics_columns):
    """
    Adds grouped totals (monthly and yearly) to the DataFrame.
    """
    if not df.empty:
        df[date_column] = pd.to_datetime(df[date_column])
        monthly_totals = df.groupby(df[date_column].dt.to_period("M"))[metrics_columns].sum().reset_index()
        monthly_totals[date_column] = monthly_totals[date_column].dt.strftime('Month %Y-%m')
        
        yearly_totals = df.groupby(df[date_column].dt.to_period("Y"))[metrics_columns].sum().reset_index()
        yearly_totals[date_column] = yearly_totals[date_column].dt.strftime('Year %Y')

        return pd.concat([df, monthly_totals, yearly_totals], ignore_index=True)
    return df

def get_mongo_client(connection_string):
    """
    Create a MongoDB client using the provided connection string.
    """
    return MongoClient(connection_string)

def save_to_mongodb(client, database_name, collection_name, data):
    """
    Save data to MongoDB.
    """
    db = client[database_name]
    collection = db[collection_name]
    collection.insert_many(data)

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
    filename = args.filename or f"{args.owner}-{args.repo}-traffic-data.xlsx"

    # Fetch and process data
    views_data = get_github_data(f"{base_url}/traffic/views", token)
    clones_data = get_github_data(f"{base_url}/traffic/clones", token)
    referrers_data = get_github_data(f"{base_url}/traffic/popular/referrers", token)
    popular_content_data = get_github_data(f"{base_url}/traffic/popular/paths", token)

    traffic_df = append_new_data(pd.DataFrame(), process_traffic_data(views_data), 'Date')
    clones_df = append_new_data(pd.DataFrame(), process_clones_data(clones_data), 'Date')
    referrers_df = append_new_data(pd.DataFrame(), process_referrers_data(referrers_data), 'Referring site')
    popular_content_df = append_new_data(pd.DataFrame(), process_popular_content_data(popular_content_data), 'Path')

    # Add grouped totals for Traffic Stats and Git Clones
    traffic_df = add_grouped_totals(traffic_df, 'Date', ['Views', 'Unique visitors'])
    clones_df = add_grouped_totals(clones_df, 'Date', ['Clones', 'Unique cloners'])

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
        database_name = "GitHubTrafficData"
        save_to_mongodb(mongo_client, database_name, "TrafficStats", traffic_df.to_dict('records'))
        save_to_mongodb(mongo_client, database_name, "GitClones", clones_df.to_dict('records'))
        save_to_mongodb(mongo_client, database_name, "ReferringSites", referrers_df.to_dict('records'))
        save_to_mongodb(mongo_client, database_name, "PopularContent", popular_content_df.to_dict('records'))

        print("Data also saved to MongoDB")

if __name__ == "__main__":
    main()