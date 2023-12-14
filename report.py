import requests
import pandas as pd
import argparse
from datetime import datetime
from pymongo import MongoClient

# Function to fetch data from GitHub API
def get_github_data(api_url, token):
    headers = {'Authorization': f'token {token}'}
    response = requests.get(api_url, headers=headers)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Failed to fetch data: {response.status_code} - {response.text}")
        return None

# Function to process traffic data
def process_traffic_data(data):
    return [{"Date": datetime.strptime(item['timestamp'][:10], '%Y-%m-%d'), 
             "Views": item['count'], 
             "Unique visitors": item['uniques']} for item in data['views']]

# Function to process clones data
def process_clones_data(data):
    return [{"Date": datetime.strptime(item['timestamp'][:10], '%Y-%m-%d'), 
             "Clones": item['count'], 
             "Unique cloners": item['uniques']} for item in data['clones']]

# Function to process referrers data with timestamp
def process_referrers_data(data):
    timestamp = datetime.now()
    return [{"Referring site": item['referrer'], 
             "Views": item['count'], 
             "Unique visitors": item['uniques'],
             "FetchedAt": timestamp} for item in data]

# Function to process popular content data with timestamp
def process_popular_content_data(data):
    timestamp = datetime.now()
    return [{"Path": item['path'], 
             "Views": item['count'], 
             "Unique visitors": item['uniques'],
             "FetchedAt": timestamp} for item in data]

# Function to read the Personal Access Token from a file
def read_token_from_file(file_path):
    try:
        with open(file_path, 'r') as file:
            return file.read().strip()
    except IOError as e:
        print(f"Error reading token file: {e}")
        return None

# Function to fetch historical data from MongoDB
def fetch_historical_data(client, database_name, collection_name):
    db = client[database_name]
    collection = db[collection_name]
    data = list(collection.find())
    return pd.DataFrame(data)

# Function to get the last recorded date from a DataFrame
def get_last_recorded_date(df, date_column='Date'):
    if df.empty or date_column not in df.columns:
        return None
    df[date_column] = pd.to_datetime(df[date_column], errors='coerce')
    return df[date_column].max()

# Function to append new data to the DataFrame
def append_new_data(old_df, new_data, date_column):
    new_df = pd.DataFrame(new_data)
    if not old_df.empty:
        last_date = get_last_recorded_date(old_df, date_column)
        if last_date is not None:
            new_df = new_df[pd.to_datetime(new_df[date_column]) > last_date]
    return pd.concat([old_df, new_df], ignore_index=True)

# Function to create a MongoDB client
def get_mongo_client(connection_string):
    return MongoClient(connection_string)

def save_to_mongodb(client, database_name, collection_name, data):
    db = client[database_name]
    collection = db[collection_name]

    for item in data:
        item.pop('_id', None)  # Remove the '_id' field if it exists
        # Handle NaT values in the 'Date' field
        if pd.isna(item.get('Date')):
            continue  # Skip this record

        query = {'Date': item['Date']}
        update = {"$set": item}
        collection.update_one(query, update, upsert=True)

# Function to calculate daily, monthly, and yearly totals
def calculate_daily_monthly_yearly_totals(df, date_column, count_column):
    df[date_column] = pd.to_datetime(df[date_column], errors='coerce')
    df = df.dropna(subset=[date_column])
    df = df.sort_values(by=date_column).reset_index(drop=True)

    # Calculate daily, monthly, and yearly totals
    daily_totals = df.groupby(df[date_column].dt.to_period("D"))[count_column].sum()
    monthly_totals = df.groupby(df[date_column].dt.to_period("M"))[count_column].sum()
    yearly_totals = df.groupby(df[date_column].dt.to_period("Y"))[count_column].sum()

    # Create a DataFrame to hold the summary rows
    summary_df = pd.DataFrame({
        date_column: [df[date_column].min()],  # Date of the first data point
        count_column: [daily_totals.sum()],
    })

    # Append the summary rows for each month and year
    for period, total in monthly_totals.items():
        summary_df = pd.concat([summary_df, pd.DataFrame({
            date_column: [period.to_timestamp()],  # Start of the month
            count_column: [total],
        })], ignore_index=True)
    for period, total in yearly_totals.items():
        summary_df = pd.concat([summary_df, pd.DataFrame({
            date_column: [period.to_timestamp()],  # Start of the year
            count_column: [total],
        })], ignore_index=True)

    return pd.concat([df, summary_df], ignore_index=True)

# Main function with command-line interface
def main():
    parser = argparse.ArgumentParser(description='GitHub Repository Traffic Data Fetcher')
    parser.add_argument('--repo', required=True, help='Repository name')
    parser.add_argument('--owner', required=True, help='Organization/user name that owns the repository')
    parser.add_argument('--filename', help='Optional: Specify a filename for the Excel output. If not provided, defaults to {owner}-{repo}-traffic-data.xlsx')
    parser.add_argument('--token-file', required=True, help='Path to a text file containing the GitHub Personal Access Token')
    parser.add_argument('--mongodb-connection-string', required=True, help='MongoDB connection string to store and retrieve data')
    args = parser.parse_args()

    token = read_token_from_file(args.token_file)
    if not token:
        return

    base_url = f"https://api.github.com/repos/{args.owner}/{args.repo}"
    filename = args.filename or f"{args.owner}-{args.repo}-traffic-data.xlsx"

    mongo_client = get_mongo_client(args.mongodb_connection_string)
    database_name = "GitHubTrafficData"

    historical_traffic_df = fetch_historical_data(mongo_client, database_name, "TrafficStats")
    historical_clones_df = fetch_historical_data(mongo_client, database_name, "GitClones")
    historical_referrers_df = fetch_historical_data(mongo_client, database_name, "ReferringSites")
    historical_popular_content_df = fetch_historical_data(mongo_client, database_name, "PopularContent")

    views_data = get_github_data(f"{base_url}/traffic/views", token)
    clones_data = get_github_data(f"{base_url}/traffic/clones", token)
    referrers_data = get_github_data(f"{base_url}/traffic/popular/referrers", token)
    popular_content_data = get_github_data(f"{base_url}/traffic/popular/paths", token)

    traffic_df = append_new_data(historical_traffic_df, process_traffic_data(views_data), 'Date')
    clones_df = append_new_data(historical_clones_df, process_clones_data(clones_data), 'Date')
    referrers_df = append_new_data(historical_referrers_df, process_referrers_data(referrers_data), 'FetchedAt')
    popular_content_df = append_new_data(historical_popular_content_df, process_popular_content_data(popular_content_data), 'FetchedAt')

    # Calculate daily, monthly, and yearly totals for the 'traffic_df'
    traffic_df = calculate_daily_monthly_yearly_totals(traffic_df, 'Date', 'Views')

    # Save data to Excel and MongoDB
    with pd.ExcelWriter(filename, engine='openpyxl') as writer:
        traffic_df.to_excel(writer, sheet_name='Traffic Stats', index=False)
        clones_df.to_excel(writer, sheet_name='Git Clones', index=False)
        referrers_df.to_excel(writer, sheet_name='Referring Sites', index=False)
        popular_content_df.to_excel(writer, sheet_name='Popular Content', index=False)

    print(f"Data successfully saved to {filename}")

    save_to_mongodb(mongo_client, database_name, "TrafficStats", traffic_df.to_dict('records'))
    save_to_mongodb(mongo_client, database_name, "GitClones", clones_df.to_dict('records'))
    save_to_mongodb(mongo_client, database_name, "ReferringSites", referrers_df.to_dict('records'))
    save_to_mongodb(mongo_client, database_name, "PopularContent", popular_content_df.to_dict('records'))

    print("Data also saved to MongoDB")

# Call to main function
if __name__ == "__main__":
    main()