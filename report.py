import os
import requests
import pandas as pd
import argparse
from datetime import datetime, timedelta
from pymongo import MongoClient
from openpyxl.styles import Font, Border, Side
from azure.storage.blob import BlobServiceClient, BlobSasPermissions, \
    generate_blob_sas


# Retrieves data from GitHub API using a URL and token.
# Handles HTTP responses and returns JSON data if successful.
def get_github_data(api_url, token):
    headers = {'Authorization': f'token {token}'}
    response = requests.get(api_url, headers=headers)
    if response.status_code == 200:
        return response.json()
    else:
        error_message = (
            "Failed to fetch data: "
            f"{response.status_code} - {response.text}"
        )
        print(error_message)
        return None


# Fetches stargazer data for a GitHub repo.
# Handles pagination for large datasets.
def get_stars_data(api_url, token):
    stars_data = []
    page = 1
    while True:
        headers = {
            'Authorization': f'token {token}',
            'Accept': 'application/vnd.github.v3.star+json'
        }
        url = f"{api_url}/stargazers?page={page}&per_page=100"
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            data = response.json()
            if data:
                for entry in data:
                    starred_at = entry.get('starred_at', '')
                    user_login = entry.get('user', {}).get('login', 'Unknown')
                    stars_data.append({
                        'starred_at': starred_at,
                        'login': user_login
                    })
                page += 1
            else:
                break
        else:
            error_msg = (
                f"Failed to fetch stars data: "
                f"{response.status_code} - {response.text}"
            )
            print(error_msg)
            break
    return stars_data


# Processes star data, calculating cumulative count per date.
def process_stars_data(stars_data):
    cumulative_count = {}
    total_stars = 0
    sorted_data = sorted(stars_data, key=lambda x: x['starred_at'])

    for star_info in sorted_data:
        date = star_info['starred_at'].split('T')[0]
        total_stars += 1
        cumulative_count[date] = total_stars

    processed_data = [
        {"Date": date, "Total Stars": count}
        for date, count in cumulative_count.items()
    ]

    return processed_data


# Retrieves fork data for a GitHub repo,
# managing pagination for large sets.
def get_forks_data(api_url, token):
    forks_data = []
    page = 1
    while True:
        headers = {
            'Authorization': f'token {token}',
            'Accept': 'application/vnd.github.v3+json'
        }
        url = f"{api_url}/forks?page={page}&per_page=100"
        response = requests.get(url, headers=headers)
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
            error_msg = (
                f"Failed to fetch forks data: "
                f"{response.status_code} - {response.text}"
            )
            print(error_msg)
            break
    return forks_data


# Processes fork data, calculating cumulative count per date.
def process_forks_data(forks_data):
    cumulative_count = {}
    total_forks = 0
    sorted_data = sorted(forks_data, key=lambda x: x['created_at'])

    for fork_info in sorted_data:
        date = fork_info['created_at'].split('T')[0]
        total_forks += 1
        cumulative_count[date] = total_forks

    processed_data = [
        {"Date": date, "Total Forks": count}
        for date, count in cumulative_count.items()
    ]

    return processed_data


# Converts traffic data into structured format
# with date, views, and unique visitors.
def process_traffic_data(data):
    return [
        {"Date": datetime.strptime(item['timestamp'][:10], '%Y-%m-%d').date(),
         "Views": item['count'],
         "Unique visitors": item['uniques']}
        for item in data['views']
    ]


# Converts clone data into structured format
# with date, count, and unique cloners.
def process_clones_data(data):
    return [
        {"Date": datetime.strptime(item['timestamp'][:10], '%Y-%m-%d').date(),
         "Clones": item['count'],
         "Unique cloners": item['uniques']}
        for item in data['clones']
    ]


# Processes referrer data into structured format
# with site, views, and unique visitors.
def process_referrers_data(data):
    timestamp = datetime.now()
    return [
        {"Referring site": item['referrer'],
         "Views": item['count'],
         "Unique visitors": item['uniques'],
         "FetchedAt": timestamp}
        for item in data
    ]


# Converts popular content data into structured
# format with path, views, and visitors.
def process_popular_content_data(data):
    timestamp = datetime.now()
    return [
        {"Path": item['path'],
         "Views": item['count'],
         "Unique visitors": item['uniques'],
         "FetchedAt": timestamp}
        for item in data
    ]


# Reads GitHub personal access token from a specified file.
def read_token_from_file(file_path):
    try:
        with open(file_path, 'r') as file:
            return file.read().strip()
    except IOError as e:
        print(f"Error reading token file: {e}")
        return None


# Determines the latest recorded date in a DataFrame for updates.
def get_last_recorded_date(df):
    if df.empty or "Date" not in df.columns:
        return None
    return pd.to_datetime(df["Date"]).max()


# Fetches all data from MongoDB collection, returns it as DataFrame.
def fetch_all_data_from_mongodb(client, database_name, collection_name):
    db = client[database_name]
    collection = db[collection_name]
    data = list(collection.find({}, {'_id': 0}))  # Exclude the Mongo ID
    return pd.DataFrame(data)


# Appends new data to DataFrame, avoiding duplicates, and sorts by date.
def append_new_data(mongo_client, database_name, collection_name,
                    new_data, date_column):
    old_df = fetch_all_data_from_mongodb(
        mongo_client, database_name, collection_name
    )
    new_df = pd.DataFrame(new_data)
    new_df = new_df[
        new_df[date_column].apply(lambda x: isinstance(x, datetime))
    ]
    old_df = old_df[
        old_df[date_column].apply(lambda x: isinstance(x, datetime))
    ]
    combined_df = pd.concat([old_df, new_df], ignore_index=True)
    combined_df.drop_duplicates(subset=[date_column],
                                keep='last', inplace=True)
    combined_df.sort_values(by=date_column, inplace=True)
    return combined_df


# Saves data to MongoDB, updates existing records and adds new ones.
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


# Formats Excel sheet header with bold font and thin border.
def format_excel_header(writer, sheet_name):
    workbook = writer.book
    worksheet = workbook[sheet_name]
    header_font = Font(bold=True)
    thin_border = Border(left=Side(style='thin'),
                         right=Side(style='thin'),
                         top=Side(style='thin'),
                         bottom=Side(style='thin'))

    for cell in worksheet['1:1']:  # First row is the header
        cell.font = header_font
        cell.border = thin_border


# Uploads file to Azure Blob Storage and generates temporary URL.
def upload_to_azure_blob(storage_connection_string, container_name,
                         file_path, file_name):
    blob_service_client = BlobServiceClient.from_connection_string(
                          storage_connection_string)
    container_client = blob_service_client.get_container_client(container_name)

    try:
        container_client.create_container()
    except Exception as e:
        print(f"Container already exists or error: {e}")

    blob_client = blob_service_client.get_blob_client(
        container=container_name, blob=file_name
    )
    with open(file_path, "rb") as data:
        blob_client.upload_blob(data, overwrite=True)

    sas_token = generate_blob_sas(
        account_name=blob_service_client.account_name,
        container_name=container_name,
        blob_name=file_name,
        account_key=blob_service_client.credential.account_key,
        permission=BlobSasPermissions(read=True),
        expiry=datetime.utcnow() + timedelta(hours=24)
    )

    return (
        f"https://{blob_service_client.account_name}"
        f".blob.core.windows.net/{container_name}/{file_name}"
        f"?{sas_token}"
    )


# Sanitizes container name for Azure Blob Storage, replaces invalid chars.
def sanitize_container_name(name):
    return ''.join(char if char.isalnum() else '-' for char in name).lower()


# Creates and returns MongoDB client using connection string.
def get_mongo_client(connection_string):
    return MongoClient(connection_string)


# Main function: sets up CLI arguments and executes script logic.
def main():
    parser = argparse.ArgumentParser(
        description='GitHub Repository Traffic Data Fetcher'
    )
    parser.add_argument('--repo', required=True, help='Repository name')
    parser.add_argument('--owner', required=True,
                        help='Organization/user name that owns the repository')
    parser.add_argument(
        '--filename',
        help=('Optional: Specify a filename for the Excel output. '
              'If not provided, defaults to {owner}-{repo}-traffic-data.xlsx')
    )
    parser.add_argument(
        '--token-file', required=True,
        help='Path to a text file containing the GitHub Personal Access Token'
    )
    parser.add_argument(
        '--mongodb-connection-string', required=True,
        help='MongoDB connection string to store and retrieve the data'
    )
    parser.add_argument(
        '--azure-storage-connection-string',
        help=('Optional: Azure Blob Storage connection string '
              'for storing the Excel file')
    )
    args = parser.parse_args()

    token = read_token_from_file(args.token_file)
    if not token:
        return

    base_url = (
        f"https://api.github.com/repos/{args.owner}/{args.repo}"
    )
    filename = (
        args.filename or f"{args.owner}-{args.repo}-traffic-data.xlsx"
    )

    # Create MongoDB client
    mongo_client = get_mongo_client(args.mongodb_connection_string)

    # Fetch and process data from GitHub API
    views_data = get_github_data(
        f"{base_url}/traffic/views", token
    )
    clones_data = get_github_data(
        f"{base_url}/traffic/clones", token
    )
    referrers_data = get_github_data(
        f"{base_url}/traffic/popular/referrers", token
    )
    popular_content_data = get_github_data(
        f"{base_url}/traffic/popular/paths", token
    )
    stars_data = get_stars_data(base_url, token)
    forks_data = get_forks_data(base_url, token)

    # Convert the processed data to DataFrames
    referrers_df = pd.DataFrame(process_referrers_data(referrers_data))
    popular_content_df = pd.DataFrame(
        process_popular_content_data(popular_content_data)
    )
    stars_df = pd.DataFrame(process_stars_data(stars_data))
    forks_df = pd.DataFrame(process_forks_data(forks_data))

    # Process and append the new data
    traffic_df = append_new_data(
        mongo_client, args.repo, "TrafficStats",
        process_traffic_data(views_data), 'Date'
    )
    clones_df = append_new_data(
        mongo_client, args.repo, "GitClones",
        process_clones_data(clones_data), 'Date'
    )

    # Save to MongoDB
    save_to_mongodb(
        mongo_client, args.repo, "TrafficStats",
        traffic_df.to_dict('records')
    )
    save_to_mongodb(
        mongo_client, args.repo, "GitClones",
        clones_df.to_dict('records')
    )
    save_to_mongodb(
        mongo_client, args.repo, "ReferringSites",
        referrers_df.to_dict('records')
    )
    save_to_mongodb(
        mongo_client, args.repo, "PopularContent",
        popular_content_df.to_dict('records')
    )
    save_to_mongodb(
        mongo_client, args.repo, "Stars",
        stars_df.to_dict('records')
    )
    save_to_mongodb(
        mongo_client, args.repo, "Forks",
        forks_df.to_dict('records')
    )

    print("Data saved to MongoDB")

    # Save to Excel
    current_directory = os.getcwd()
    excel_file_path = os.path.join(current_directory, filename)
    with pd.ExcelWriter(excel_file_path, engine='openpyxl') as writer:
        traffic_df.to_excel(writer, sheet_name='Traffic Stats', index=False)
        format_excel_header(writer, 'Traffic Stats')
        clones_df.to_excel(writer, sheet_name='Git Clones', index=False)
        format_excel_header(writer, 'Git Clones')
        referrers_df.to_excel(
            writer, sheet_name='Referring Sites', index=False
        )
        format_excel_header(writer, 'Referring Sites')
        popular_content_df.to_excel(
            writer, sheet_name='Popular Content', index=False
        )
        format_excel_header(writer, 'Popular Content')
        stars_df.to_excel(writer, sheet_name='Stars', index=False)
        format_excel_header(writer, 'Stars')
        forks_df.to_excel(writer, sheet_name='Forks', index=False)
        format_excel_header(writer, 'Forks')

    print(f"Excel file saved locally at: {excel_file_path}")

    # Upload to Azure Blob if connection string is provided
    if args.azure_storage_connection_string:
        azure_blob_url = upload_to_azure_blob(
            args.azure_storage_connection_string,
            sanitize_container_name(args.repo),
            excel_file_path, filename
        )
        print(
            "Excel file uploaded to Azure Blob Storage. "
            "Temporary download link (valid for 24 hours): "
            f"{azure_blob_url}"
        )


if __name__ == "__main__":
    main()
