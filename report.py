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
        logger.info(f"Fetched data for repository: {owner}/{repo}")

        # Process and append the new data
        traffic_df = append_new_data(db_client, sanitized_repo, "TrafficStats", process_traffic_data(views_data, owner, repo), 'Date', db_type)
        logger.info("Processed and appended new data")

        # Save to database
        save_data(db_client, sanitized_repo, "TrafficStats", traffic_df.to_dict('records'), db_type)
        logger.info("Data saved to database")

        # Define the dataframes dictionary
        dataframes = {'TrafficStats': traffic_df}

        # Handle Azure Blob Storage uploads
        handle_azure_blob_storage(azure_storage_connection_string, dataframes, base_filename, output_format, use_managed_identity)

    except Exception as e:
        logger.error(f"An error occurred: {e}")

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
        account_name = connection_string.split(';')[1].split('=')[1]
        account_url = f"https://{account_name}.blob.core.windows.net"
        credential = DefaultAzureCredential()
        return BlobServiceClient(account_url=account_url, credential=credential)
    return BlobServiceClient.from_connection_string(connection_string)

def get_github_data(api_url, token):
    headers = {'Authorization': f'token {token}'}
    response = requests.get(api_url, headers=headers)
    if response.status_code == 200:
        return response.json()
    else:
        logger.error(f"Failed to fetch data: {response.status_code} - {response.text}")
        return None

def process_traffic_data(data, owner, repo):
    logger.info("Processing traffic data")
    if data is None:
        return []
    processed_data = [
        {"Repo Owner and Name": f"{owner}-{repo}",
         "Date": datetime.strptime(item['timestamp'][:10], '%Y-%m-%d').date(),
         "Views": item['count'],
         "Unique visitors": item['uniques']}
        for item in data.get('views', [])
    ]
    logger.info(f"Processed traffic data: {len(processed_data)} records")
    return processed_data

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

def convert_to_json_serializable(data):
    """Convert data to JSON serializable format with double quotes for keys and strings."""
    if isinstance(data, pd.Timestamp):
        return data.isoformat()
    elif isinstance(data, datetime):
        return data.isoformat()
    elif isinstance(data, float) and data.is_integer():
        return int(data)
    elif isinstance(data, dict):
        return {json.dumps(str(key)): convert_to_json_serializable(value) for key, value in data.items()}
    elif isinstance(data, list):
        return [convert_to_json_serializable(item) for item in data]
    elif isinstance(data, str):
        return json.dumps(data)
    else:
        return data

def preprocess_data(data):
    """Preprocess data to ensure it matches the expected schema."""
    for item in data:
        if 'Date' in item and isinstance(item['Date'], pd.Timestamp):
            item['Date'] = item['Date'].isoformat()
        if 'Views' in item and isinstance(item['Views'], float):
            item['Views'] = int(item['Views'])
        if 'Unique visitors' in item and isinstance(item['Unique visitors'], float):
            item['Unique visitors'] = int(item['Unique visitors'])
        if 'FetchedAt' in item and isinstance(item['FetchedAt'], pd.Timestamp):
            item['FetchedAt'] = item['FetchedAt'].isoformat()
    return data

def validate_and_convert_data(data, schema_validator):
    """Validate and convert data to JSON serializable format."""
    data = preprocess_data(data)
    if not schema_validator(data):
        raise ValueError(f"Invalid data schema: {data}")
    return [convert_to_json_serializable(item) for item in data]

def validate_traffic_stats_schema(data):
    """Validate the schema of TrafficStats data."""
    required_fields = ['Date', 'Views', 'Unique visitors', 'Repo Owner and Name']
    for item in data:
        for field in required_fields:
            if field not in item:
                logger.error(f"Missing field '{field}' in item: {item}")
                return False
            if field == 'Date' and not isinstance(item[field], str):
                logger.error(f"Invalid type for 'Date' in item: {item}")
                return False
            if field == 'Views' and not isinstance(item[field], int):
                logger.error(f"Invalid type for 'Views' in item: {item}")
                return False
            if field == 'Unique visitors' and not isinstance(item[field], int):
                logger.error(f"Invalid type for 'Unique visitors' in item: {item}")
                return False
            if field == 'Repo Owner and Name' and not isinstance(item[field], str):
                logger.error(f"Invalid type for 'Repo Owner and Name' in item: {item}")
                return False
    return True

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
