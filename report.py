import os
import requests
import pandas as pd
from datetime import datetime, timedelta
from azure.storage.blob import BlobServiceClient, BlobSasPermissions, generate_blob_sas
from azure.core.exceptions import ResourceExistsError
from azure.identity import DefaultAzureCredential
import logging
import argparse
from io import BytesIO
from openpyxl.styles import Font

from db import get_mongo_client, get_cosmos_client, append_new_data, save_data

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def sanitize_name(name):
    """Replaces non-alphanumeric characters with dashes and converts to lowercase."""
    return ''.join(char if char.isalnum() else '-' for char in name).lower()

def retrieve_and_process_stats(owner, repo, filename,
                               db_connection_string, db_type,
                               azure_storage_connection_string,
                               output_format, token, use_managed_identity):
    try:
        base_url = f"https://api.github.com/repos/{owner}/{repo}"
        sanitized_repo = sanitize_name(repo)
        filename = filename or f"{owner}-{sanitized_repo}-traffic-data"
        base_filename = os.path.splitext(filename)[0]

        logger.info("Starting data retrieval and processing")
        logger.info(f"Repository: {owner}/{repo}")
        logger.info(f"Filename: {filename}")
        logger.info(f"Database type: {db_type}")

        # Create database client
        db_client = get_db_client(db_connection_string, db_type)

        # Fetch and process data from GitHub API
        views_data = get_github_data(f"{base_url}/traffic/views", token)
        clones_data = get_github_data(f"{base_url}/traffic/clones", token)
        referrers_data = get_github_data(f"{base_url}/traffic/popular/referrers", token)
        popular_content_data = get_github_data(f"{base_url}/traffic/popular/paths", token)
        stars_data = get_stars_data(base_url, token)
        forks_data = get_forks_data(base_url, token)

        logger.info(f"Fetched data for repository: {owner}/{repo}")

        # Convert the processed data to DataFrames
        referrers_df = pd.DataFrame(process_referrers_data(referrers_data, owner, repo))
        popular_content_df = pd.DataFrame(process_popular_content_data(popular_content_data, owner, repo))
        stars_df = pd.DataFrame(process_stars_data(stars_data, owner, repo))
        forks_df = pd.DataFrame(process_forks_data(forks_data, owner, repo))

        # Process and append the new data
        traffic_df = append_new_data(db_client, sanitized_repo, "TrafficStats", process_traffic_data(views_data, owner, repo), 'Date', db_type)
        clones_df = append_new_data(db_client, sanitized_repo, "GitClones", process_clones_data(clones_data, owner, repo), 'Date', db_type)

        # Save to database
        ensure_collection_exists(db_client, sanitized_repo, "TrafficStats", db_type)
        save_data(db_client, sanitized_repo, "TrafficStats", traffic_df.to_dict('records'), db_type)
        ensure_collection_exists(db_client, sanitized_repo, "GitClones", db_type)
        save_data(db_client, sanitized_repo, "GitClones", clones_df.to_dict('records'), db_type)
        ensure_collection_exists(db_client, sanitized_repo, "ReferringSites", db_type)
        save_data(db_client, sanitized_repo, "ReferringSites", referrers_df.to_dict('records'), db_type)
        ensure_collection_exists(db_client, sanitized_repo, "PopularContent", db_type)
        save_data(db_client, sanitized_repo, "PopularContent", popular_content_df.to_dict('records'), db_type)
        ensure_collection_exists(db_client, sanitized_repo, "Stars", db_type)
        save_data(db_client, sanitized_repo, "Stars", stars_df.to_dict('records'), db_type)
        ensure_collection_exists(db_client, sanitized_repo, "Forks", db_type)
        save_data(db_client, sanitized_repo, "Forks", forks_df.to_dict('records'), db_type)

        logger.info("Data saved to database")

        # Define the dataframes dictionary
        dataframes = {
            'TrafficStats': traffic_df,
            'GitClones': clones_df,
            'ReferringSites': referrers_df,
            'PopularContent': popular_content_df,
            'Stars': stars_df,
            'Forks': forks_df
        }

        # Handle Azure Blob Storage uploads
        handle_azure_blob_storage(azure_storage_connection_string, dataframes, base_filename, output_format, use_managed_identity)

    except Exception as e:
        logger.error(f"An error occurred: {e}")

def ensure_collection_exists(db_client, repo, collection_name, db_type):
    """Ensure that the collection exists in the database."""
    if db_type == "mongodb":
        db = db_client[repo]
        if collection_name not in db.list_collection_names():
            db.create_collection(collection_name)
    elif db_type == "cosmosdb":
        db = db_client.get_database_client(repo)
        try:
            db.create_container_if_not_exists(id=collection_name, partition_key=PartitionKey(path='/id'))
        except Exception as e:
            logger.error(f"Error creating collection '{collection_name}': {e}")

def get_db_client(connection_string, db_type):
    """Get the appropriate database client based on the database type."""
    if db_type == "mongodb":
        return get_mongo_client(connection_string)
    elif db_type == "cosmosdb":
        return get_cosmos_client(connection_string)
    else:
        raise ValueError(f"Unsupported database type: {db_type}")

def handle_azure_blob_storage(connection_string, dataframes, base_filename, output_format, use_managed_identity):
    """Handle uploading dataframes to Azure Blob Storage."""
    if connection_string:
        container_name = sanitize_name(base_filename)

        # Upload JSON files directly to Azure Blob Storage
        if output_format in ['json', 'all']:
            upload_json_to_azure_blob(connection_string, dataframes, base_filename, container_name, use_managed_identity)

        # Upload Excel file directly to Azure Blob Storage
        if output_format in ['excel', 'all']:
            upload_excel_to_azure_blob(connection_string, dataframes, base_filename, container_name, use_managed_identity)
    else:
        save_files_locally(dataframes, base_filename, output_format)

def upload_json_to_azure_blob(connection_string, dataframes, base_filename, container_name, use_managed_identity):
    """Upload JSON dataframes to Azure Blob Storage."""
    for df_name, df in dataframes.items():
        json_bytes = BytesIO()
        df.to_json(json_bytes, orient='records', date_format='iso')
        json_bytes.seek(0)
        json_file_name = f"{base_filename}-{df_name}.json"
        azure_blob_url = upload_to_azure_blob_stream(
            connection_string,
            container_name,
            json_bytes,
            json_file_name,
            directory='json/',
            use_managed_identity=use_managed_identity
        )
        logger.info(f"JSON file uploaded to Azure Blob Storage: {azure_blob_url}")

def upload_excel_to_azure_blob(connection_string, dataframes, base_filename, container_name, use_managed_identity):
    """Upload Excel dataframes to Azure Blob Storage."""
    excel_bytes = BytesIO()
    with pd.ExcelWriter(excel_bytes, engine='openpyxl') as writer:
        for df_name, df in dataframes.items():
            df.to_excel(writer, sheet_name=df_name, index=False)
            format_excel_header(writer, df_name)
    excel_bytes.seek(0)
    excel_file_name = f"{base_filename}.xlsx"
    azure_blob_url = upload_to_azure_blob_stream(
        connection_string,
        container_name,
        excel_bytes,
        excel_file_name,
        directory='excel/',
        use_managed_identity=use_managed_identity
    )
    logger.info(f"Excel file uploaded to Azure Blob Storage: {azure_blob_url}")

def save_files_locally(dataframes, base_filename, output_format):
    """Save dataframes to local files."""
    output_directory = "output"
    if not os.path.exists(output_directory):
        os.makedirs(output_directory)

    if output_format in ['excel', 'all']:
        excel_file_path = os.path.join(output_directory, f"{base_filename}.xlsx")
        with pd.ExcelWriter(excel_file_path, engine='openpyxl') as writer:
            for df_name, df in dataframes.items():
                df.to_excel(writer, sheet_name=df_name, index=False)
                format_excel_header(writer, df_name)
        logger.info(f"Excel file saved locally at: {excel_file_path}")

    if output_format in ['json', 'all']:
        for df_name, df in dataframes.items():
            json_file_path = os.path.join(output_directory, f"{base_filename}-{df_name}.json")
            df.to_json(json_file_path, orient='records', date_format='iso')
            logger.info(f"JSON file saved locally at: {json_file_path}")

def upload_to_azure_blob_stream(connection_string, container_name, stream, blob_name, directory='', use_managed_identity=False):
    try:
        blob_service_client = create_blob_service_client(connection_string, use_managed_identity)
        container_client = blob_service_client.get_container_client(container_name)

        try:
            container_client.create_container()
        except ResourceExistsError:
            logger.info(f"Container '{container_name}' already exists.")
        except Exception as e:
            logger.error(f"Error creating container: {e}")
            return

        full_blob_name = f"{directory}{blob_name}" if directory else blob_name
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=full_blob_name)
        blob_client.upload_blob(stream, overwrite=True)

        if not use_managed_identity:
            sas_token = generate_blob_sas(
                account_name=blob_service_client.account_name,
                container_name=container_name,
                blob_name=full_blob_name,
                account_key=blob_service_client.credential.account_key,
                permission=BlobSasPermissions(read=True),
                expiry=datetime.utcnow() + timedelta(hours=24)
            )
            return f"https://{blob_service_client.account_name}.blob.core.windows.net/{container_name}/{full_blob_name}?{sas_token}"
        else:
            return f"https://{blob_service_client.account_name}.blob.core.windows.net/{container_name}/{full_blob_name}"
    except Exception as e:
        logger.error(f"An error occurred while uploading to Azure Blob Storage: {e}")

def create_blob_service_client(connection_string, use_managed_identity=False):
    if use_managed_identity:
        credential = DefaultAzureCredential()
        return BlobServiceClient(account_url=connection_string, credential=credential)
    return BlobServiceClient.from_connection_string(connection_string)

def get_github_data(api_url, token):
    headers = {'Authorization': f'token {token}'}
    response = requests.get(api_url, headers=headers)
    if response.status_code == 200:
        return response.json()
    else:
        logger.error(f"Failed to fetch data: {response.status_code} - {response.text}")
        return None

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
                stars_data.extend(data)
                page += 1
            else:
                break
        else:
            logger.error(f"Failed to fetch stars data: {response.status_code} - {response.text}")
            break
    return stars_data

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
                forks_data.extend(data)
                page += 1
            else:
                break
        else:
            logger.error(f"Failed to fetch forks data: {response.status_code} - {response.text}")
            break
    return forks_data

def process_stars_data(stars_data, owner, repo):
    cumulative_count = {}
    total_stars = 0
    sorted_data = sorted(stars_data, key=lambda x: x['starred_at'])

    for star_info in sorted_data:
        date = star_info['starred_at'].split('T')[0]
        total_stars += 1
        cumulative_count[date] = total_stars

    processed_data = [
        {"Repo Owner and Name": f"{owner}-{repo}",
         "Date": date, "Total Stars": count}
        for date, count in cumulative_count.items()
    ]

    return processed_data

def process_forks_data(forks_data, owner, repo):
    cumulative_count = {}
    total_forks = 0
    sorted_data = sorted(forks_data, key=lambda x: x['created_at'])

    for fork_info in sorted_data:
        date = fork_info['created_at'].split('T')[0]
        total_forks += 1
        cumulative_count[date] = total_forks

    processed_data = [
        {"Repo Owner and Name": f"{owner}-{repo}",
         "Date": date, "Total Forks": count}
        for date, count in cumulative_count.items()
    ]

    return processed_data

def process_traffic_data(data, owner, repo):
    if data is None:
        return []
    return [
        {"Repo Owner and Name": f"{owner}-{repo}",
         "Date": datetime.strptime(item['timestamp'][:10], '%Y-%m-%d').date().isoformat(),
         "Views": item['count'],
         "Unique visitors": item['uniques']}
        for item in data.get('views', [])
    ]

def process_clones_data(data, owner, repo):
    if data is None:
        return []
    return [
        {"Repo Owner and Name": f"{owner}-{repo}",
         "Date": datetime.strptime(item['timestamp'][:10], '%Y-%m-%d').date().isoformat(),
         "Clones": item['count'],
         "Unique cloners": item['uniques']}
        for item in data['clones']
    ]

def process_referrers_data(data, owner, repo):
    if data is None:
        return []
    timestamp = datetime.now()
    return [
        {"Repo Owner and Name": f"{owner}-{repo}",
         "Referring site": item['referrer'],
         "Views": item['count'],
         "Unique visitors": item['uniques'],
         "FetchedAt": timestamp}
        for item in data
    ]

def process_popular_content_data(data, owner, repo):
    if data is None:
        return []
    timestamp = datetime.now()
    return [
        {"Repo Owner and Name": f"{owner}-{repo}",
         "Path": item['path'],
         "Views": item['count'],
         "Unique visitors": item['uniques'],
         "FetchedAt": timestamp}
        for item in data
    ]

def read_token_from_file(file_path):
    try:
        with open(file_path, 'r') as file:
            return file.read().strip()
    except IOError as e:
        logger.error(f"Error reading token file: {e}")
        return None

def format_excel_header(writer, sheet_name):
    workbook = writer.book
    worksheet = workbook[sheet_name]
    header_font = Font(bold=True)
    for cell in worksheet['1:1']:  # First row is the header
        cell.font = header_font

def main():
    parser = argparse.ArgumentParser(description='GitHub Repository Traffic Data Fetcher')
    parser.add_argument('--repo', required=True, help='Repository name')
    parser.add_argument('--owner', required=True, help='Organization/user name that owns the repository')
    parser.add_argument('--output-format', choices=['excel', 'json', 'all'], default='excel', help='Output format for the data (excel, json, or all)')
    parser.add_argument('--filename', help='Optional: Specify a filename for the output. If not provided, defaults to {owner}-{repo}-traffic-data')
    parser.add_argument('--token-file', required=True, help='Path to a text file containing the GitHub Personal Access Token')
    parser.add_argument('--db-connection-string', required=True, help='Database connection string to store and retrieve the data')
    parser.add_argument('--db-type', choices=['mongodb', 'cosmosdb'], required=True, help='Type of database (mongodb or cosmosdb)')
    parser.add_argument('--azure-storage-connection-string', help='Optional: Azure Blob Storage connection string for storing the output file')
    parser.add_argument('--managed-identity-storage', required=False, action='store_true', help='Use Managed Identity for Azure Blob Storage authentication')

    args = parser.parse_args()

    token = read_token_from_file(args.token_file)
    if not token:
        logger.error("Failed to read GitHub token.")
        return

    retrieve_and_process_stats(args.owner, args.repo, args.filename, args.db_connection_string, args.db_type, args.azure_storage_connection_string, args.output_format, token, args.managed_identity_storage)

if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    main()
