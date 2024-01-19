import os
import requests
import pandas as pd
import argparse
from datetime import datetime, timedelta
from pymongo import MongoClient
from openpyxl.styles import Font, Border, Side
from azure.storage.blob import BlobServiceClient, BlobSasPermissions, generate_blob_sas

# Function to fetch data from GitHub API
def get_github_data(api_url, token):
    headers = {'Authorization': f'token {token}'}
    response = requests.get(api_url, headers=headers)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Failed to fetch data: {response.status_code} - {response.text}")
        return None

def get_stars_data(api_url, token):
    stars_data = []
    page = 1
    while True:
        headers = {
            'Authorization': f'token {token}',
            'Accept': 'application/vnd.github.v3.star+json'  # Ensuring we get the 'starred_at' field
        }
        response = requests.get(f"{api_url}/stargazers?page={page}&per_page=100", headers=headers)
        if response.status_code == 200:
            data = response.json()
            if data:
                # Process each stargazer entry
                for entry in data:
                    starred_at = entry.get('starred_at', '')
                    user_login = entry.get('user', {}).get('login', 'Unknown')  # Safely access the 'login'
                    # Append the processed data
                    stars_data.append({
                        'starred_at': starred_at,
                        'login': user_login
                    })
                page += 1
            else:
                break  # No more pages of data
        else:
            print(f"Failed to fetch stars data: {response.status_code} - {response.text}")
            break
    return stars_data


def process_stars_data(stars_data):
    processed_data = []
    for star_info in stars_data:
        # Assuming star_info is a dictionary with a 'user' key, which is another dictionary
        # that contains the 'login' key for the username.
        user_login = star_info['user']['login']  # Directly accessing 'login'

        # Extract the 'starred_at' date and format it
        starred_at = star_info['starred_at'].split('T')[0]

        processed_data.append({
            "Date": starred_at,
            "User": user_login
        })

    # Append the total number of stars at the end of the processed data
    total_stars = len(processed_data)
    processed_data.append({"Date": "Total", "User": "Total", "Total Stars": total_stars})

    return processed_data


def get_forks_data(api_url, token):
    forks_data = []
    page = 1
    while True:
        headers = {
            'Authorization': f'token {token}',
            'Accept': 'application/vnd.github.v3+json'
        }
        response = requests.get(f"{api_url}/forks?page={page}&per_page=100", headers=headers)
        if response.status_code == 200:
            data = response.json()
            if data:
                for entry in data:
                    created_at = entry.get('created_at', '')
                    user_login = entry.get('owner', {}).get('login', 'Unknown')
                    forks_data.append({
                        'created_at': created_at,
                        'login': user_login
                    })
                page += 1
            else:
                break
        else:
            print(f"Failed to fetch forks data: {response.status_code} - {response.text}")
            break
    return forks_data

    
def process_stars_data(data):
    processed_data = []

    for item in data:
        user_login = item.get('login', 'Unknown')  # Extract the 'login' directly from the item
        starred_at = item.get('starred_at', '').split('T')[0] if 'starred_at' in item else 'Unknown Date'
        processed_data.append({"Date": starred_at, "User": user_login})

    # Count the total number of stars
    total_stars = len(processed_data)
    # Add total stars count as a separate dictionary at the end
    processed_data.append({"Total Stars": total_stars, "User": "Total"})

    return processed_data


def process_forks_data(forks_data):
    processed_data = []
    for fork_info in forks_data:
        user_login = fork_info['login']
        created_at = fork_info['created_at'].split('T')[0]  # Only the date part

        processed_data.append({
            "Date": created_at,
            "User": user_login
        })

    # Append the total number of forks at the end of the processed data
    total_forks = len(processed_data)
    processed_data.append({"Date": "Total", "User": "Total", "Total Forks": total_forks})

    return processed_data



# Function to process traffic data
def process_traffic_data(data):
    return [{"Date": datetime.strptime(item['timestamp'][:10], '%Y-%m-%d').date(),
             "Views": item['count'],
             "Unique visitors": item['uniques']} for item in data['views']]

# Function to process clones data
def process_clones_data(data):
    return [{"Date": datetime.strptime(item['timestamp'][:10], '%Y-%m-%d').date(),
             "Clones": item['count'],
             "Unique cloners": item['uniques']} for item in data['clones']]

# Function to process referrers data
def process_referrers_data(data):
    timestamp = datetime.now()
    return [{"Referring site": item['referrer'],
             "Views": item['count'],
             "Unique visitors": item['uniques'],
             "FetchedAt": timestamp} for item in data]

# Function to process popular content data
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

# Function to get the last recorded date from a DataFrame
def get_last_recorded_date(df):
    if df.empty or "Date" not in df.columns:
        return None
    return pd.to_datetime(df["Date"]).max()

# Function to fetch all existing data from MongoDB
def fetch_all_data_from_mongodb(client, database_name, collection_name):
    db = client[database_name]
    collection = db[collection_name]
    data = list(collection.find({}, {'_id': 0}))  # Exclude the Mongo-generated ID field
    return pd.DataFrame(data)

# Updated function to append new data to the DataFrame
def append_new_data(mongo_client, database_name, collection_name, new_data, date_column):
    old_df = fetch_all_data_from_mongodb(mongo_client, database_name, collection_name)
    new_df = pd.DataFrame(new_data)
    # Convert date_column to datetime for proper comparison, filtering out non-standard dates
    new_df = new_df[new_df[date_column].apply(lambda x: isinstance(x, datetime))]
    old_df = old_df[old_df[date_column].apply(lambda x: isinstance(x, datetime))]
    combined_df = pd.concat([old_df, new_df], ignore_index=True)
    combined_df.drop_duplicates(subset=[date_column], keep='last', inplace=True)  # Remove duplicates
    combined_df.sort_values(by=date_column, inplace=True)  # Sort by date
    return combined_df

# Updated function to add grouped totals with optional referrer or content data handling
def add_grouped_totals(df, date_column, metrics_columns):
    if not df.empty:
        # Ensure handling for referrer or content data
        if 'FetchedAt' in df.columns:
            date_column = 'FetchedAt'
        
        df[date_column] = pd.to_datetime(df[date_column])
        monthly_totals = df.groupby(df[date_column].dt.to_period("M"))[metrics_columns].sum().reset_index()
        monthly_totals[date_column] = monthly_totals[date_column].dt.strftime('Month %Y-%m')
        yearly_totals = df.groupby(df[date_column].dt.to_period("Y"))[metrics_columns].sum().reset_index()
        yearly_totals[date_column] = yearly_totals[date_column].dt.strftime('Year %Y')
        return pd.concat([df, monthly_totals, yearly_totals], ignore_index=True)
    return df

# Updated function to dynamically generate the database name
def save_to_mongodb(client, database_name, collection_name, data):
    db = client[database_name]
    collection = db[collection_name]
    
    # Determine the unique field based on the collection
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

# Function to create a MongoDB client
def get_mongo_client(connection_string):
    return MongoClient(connection_string)

# Function to format Excel header
def format_excel_header(writer, sheet_name):
    workbook = writer.book
    worksheet = workbook[sheet_name]
    header_font = Font(bold=True)
    thin_border = Border(left=Side(style='thin'),
                         right=Side(style='thin'),
                         top=Side(style='thin'),
                         bottom=Side(style='thin'))

    for cell in worksheet['1:1']:  # Assuming the first row is the header
        cell.font = header_font
        cell.border = thin_border

# Function to upload file to Azure Blob Storage and get temporary URL
def upload_to_azure_blob(storage_connection_string, container_name, file_path, file_name):
    blob_service_client = BlobServiceClient.from_connection_string(storage_connection_string)
    container_client = blob_service_client.get_container_client(container_name)

    # Create container if it doesn't exist
    try:
        container_client.create_container()
    except Exception as e:
        print(f"Container already exists or error: {e}")

    # Upload the file
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=file_name)
    with open(file_path, "rb") as data:
        blob_client.upload_blob(data, overwrite=True)

    # Create a temporary URL for the blob
    sas_token = generate_blob_sas(
        account_name=blob_service_client.account_name,
        container_name=container_name,
        blob_name=file_name,
        account_key=blob_service_client.credential.account_key,
        permission=BlobSasPermissions(read=True),
        expiry=datetime.utcnow() + timedelta(hours=24)
    )

    return f"https://{blob_service_client.account_name}.blob.core.windows.net/{container_name}/{file_name}?{sas_token}"

def sanitize_container_name(name):
    # Convert to lowercase and replace invalid characters (e.g., spaces) with a dash
    return ''.join(char if char.isalnum() else '-' for char in name).lower()

def main():
    parser = argparse.ArgumentParser(description='GitHub Repository Traffic Data Fetcher')
    parser.add_argument('--repo', required=True, help='Repository name')
    parser.add_argument('--owner', required=True, help='Organization/user name that owns the repository')
    parser.add_argument('--filename', help='Optional: Specify a filename for the Excel output. If not provided, defaults to {owner}-{repo}-traffic-data.xlsx')
    parser.add_argument('--token-file', required=True, help='Path to a text file containing the GitHub Personal Access Token')
    parser.add_argument('--mongodb-connection-string', required=True, help='MongoDB connection string to store and retrieve the data')
    parser.add_argument('--azure-storage-connection-string', help='Optional: Azure Blob Storage connection string for storing the Excel file')
    args = parser.parse_args()

    token = read_token_from_file(args.token_file)
    if not token:
        return

    base_url = f"https://api.github.com/repos/{args.owner}/{args.repo}"
    filename = args.filename or f"{args.owner}-{args.repo}-traffic-data.xlsx"

    # Create MongoDB client
    mongo_client = get_mongo_client(args.mongodb_connection_string)

    # Fetch and process data from GitHub API
    views_data = get_github_data(f"{base_url}/traffic/views", token)
    clones_data = get_github_data(f"{base_url}/traffic/clones", token)
    referrers_data = get_github_data(f"{base_url}/traffic/popular/referrers", token)
    popular_content_data = get_github_data(f"{base_url}/traffic/popular/paths", token)
    stars_data = get_stars_data(base_url, token)
    forks_data = get_forks_data(base_url, token)

    # Convert the processed data to DataFrames
    referrers_df = pd.DataFrame(process_referrers_data(referrers_data))
    popular_content_df = pd.DataFrame(process_popular_content_data(popular_content_data))
    stars_df = pd.DataFrame(process_stars_data(stars_data))
    forks_df = pd.DataFrame(process_forks_data(forks_data))


    # Process and append the new data
    traffic_df = append_new_data(mongo_client, args.repo, "TrafficStats", process_traffic_data(views_data), 'Date')
    clones_df = append_new_data(mongo_client, args.repo, "GitClones", process_clones_data(clones_data), 'Date')

    # Apply add_grouped_totals to DataFrames
    traffic_df = add_grouped_totals(traffic_df, 'Date', ['Views', 'Unique visitors'])
    clones_df = add_grouped_totals(clones_df, 'Date', ['Clones', 'Unique cloners'])
    referrers_df = add_grouped_totals(referrers_df, 'FetchedAt', ['Views', 'Unique visitors'])
    popular_content_df = add_grouped_totals(popular_content_df, 'FetchedAt', ['Views', 'Unique visitors'])

    # Save to MongoDB
    save_to_mongodb(mongo_client, args.repo, "TrafficStats", traffic_df.to_dict('records'))
    save_to_mongodb(mongo_client, args.repo, "GitClones", clones_df.to_dict('records'))
    save_to_mongodb(mongo_client, args.repo, "ReferringSites", referrers_df.to_dict('records'))
    save_to_mongodb(mongo_client, args.repo, "PopularContent", popular_content_df.to_dict('records'))
    save_to_mongodb(mongo_client, args.repo, "Stars", stars_df.to_dict('records'))
    save_to_mongodb(mongo_client, args.repo, "Forks", forks_df.to_dict('records'))

    print("Data saved to MongoDB")

    # Save to Excel
    current_directory = os.getcwd()
    excel_file_path = os.path.join(current_directory, filename)
    with pd.ExcelWriter(excel_file_path, engine='openpyxl') as writer:
        traffic_df.to_excel(writer, sheet_name='Traffic Stats', index=False)
        format_excel_header(writer, 'Traffic Stats')
        clones_df.to_excel(writer, sheet_name='Git Clones', index=False)
        format_excel_header(writer, 'Git Clones')
        referrers_df.to_excel(writer, sheet_name='Referring Sites', index=False)
        format_excel_header(writer, 'Referring Sites')
        popular_content_df.to_excel(writer, sheet_name='Popular Content', index=False)
        format_excel_header(writer, 'Popular Content')
        stars_df.to_excel(writer, sheet_name='Stars', index=False)
        format_excel_header(writer, 'Stars')
        forks_df.to_excel(writer, sheet_name='Forks', index=False)
        format_excel_header(writer, 'Forks')

    print(f"Excel file saved locally at: {excel_file_path}")

    # Upload to Azure Blob if connection string is provided
    if args.azure_storage_connection_string:
        azure_blob_url = upload_to_azure_blob(args.azure_storage_connection_string, sanitize_container_name(args.repo), excel_file_path, filename)
        print(f"Excel file uploaded to Azure Blob Storage. Temporary download link (valid for 24 hours): {azure_blob_url}")

if __name__ == "__main__":
    main()
   