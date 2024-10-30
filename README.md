# GitHub Repository Traffic Data Fetcher

A Python project that retrieves and processes GitHub repository traffic data, including views, clones, stars, and forks. The data can be stored in MongoDB or Azure Cosmos DB and optionally uploaded to Azure Blob Storage. The project also includes a migration script to transfer data from MongoDB to Azure Cosmos DB.

## Table of Contents

- [Features](#features)
- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Configuration](#configuration)
- [Usage](#usage)
- [Examples](#examples)
- [Data Storage](#data-storage)
- [Migration](#migration)
- [Logging](#logging)
- [Contributing](#contributing)
- [License](#license)

## Features

- **Fetch GitHub Repository Data**:
  - Views and unique visitors
  - Clones and unique cloners
  - Stars and forks over time
  - Popular content and referring sites
- **Data Storage**:
  - Supports MongoDB and Azure Cosmos DB
  - Handles data storage and retrieval with `db.py`
- **Output Formats**:
  - Excel and JSON formats
  - Optionally uploads output files to Azure Blob Storage
- **Migration Script**:
  - `migrate.py` to transfer data from MongoDB to Azure Cosmos DB
- **Authentication**:
  - Uses GitHub Personal Access Token
  - Supports Azure Managed Identity for authentication

## Prerequisites

- **Python 3.7 or higher**
- **GitHub Personal Access Token** with appropriate permissions
- **Database Access**:
  - MongoDB or Azure Cosmos DB connection string
- **Azure Services** (optional):
  - Azure Blob Storage account
  - Azure account with permissions to use Managed Identity authentication

## Installation

1. **Clone the repository**:

   ```bash
   git clone https://github.com/yourusername/your-repo-name.git
   cd your-repo-name
   ```

2. **Install the required Python packages**:

   ```bash
   pip install -r requirements.txt
   ```

## Configuration

Before running the scripts, set up the following configurations:

- **GitHub Personal Access Token**:
  - Create a token with the necessary permissions.
  - Store it in a text file (e.g., `token.txt`).

- **Database Connection Strings**:
  - **MongoDB**: Provide your MongoDB connection string.
  - **Azure Cosmos DB**: Provide your Cosmos DB connection string.

- **Azure Storage Connection String** (optional):
  - If you want to upload output files to Azure Blob Storage, provide the connection string.

- **Managed Identity Authentication** (optional):
  - If using Managed Identity for Azure services, ensure your environment is correctly configured.

## Usage

You can run the `report.py` script to fetch and process GitHub repository traffic data.

```bash
python report.py --repo <repository_name> --owner <repository_owner> \
--token-file <path_to_token_file> --db-connection-string <db_connection_string> \
--db-type <mongodb_or_cosmosdb> [options]
```

### Required Arguments

- `--repo`: Name of the GitHub repository.
- `--owner`: Owner (user or organization) of the repository.
- `--token-file`: Path to the file containing your GitHub Personal Access Token.
- `--db-connection-string`: Connection string for MongoDB or Azure Cosmos DB.
- `--db-type`: Type of the database (`mongodb` or `cosmosdb`).

### Optional Arguments

- `--output-format`: Output format for the data (`excel`, `json`, or `all`). Default is `excel`.
- `--filename`: Custom filename for the output files.
- `--azure-storage-connection-string`: Azure Blob Storage connection string for storing output files.
- `--managed-identity-storage`: Use Managed Identity for Azure Blob Storage authentication.
- `--help`: Show help message and exit.

## Examples

### Fetch Data and Store in MongoDB

```bash
python report.py --repo my-repo --owner my-username \
--token-file token.txt --db-connection-string "mongodb://localhost:27017" \
--db-type mongodb
```

### Fetch Data and Store in Azure Cosmos DB

```bash
python report.py --repo my-repo --owner my-username \
--token-file token.txt --db-connection-string "<cosmos_db_connection_string>" \
--db-type cosmosdb
```

### Fetch Data and Upload Outputs to Azure Blob Storage

```bash
python report.py --repo my-repo --owner my-username \
--token-file token.txt --db-connection-string "mongodb://localhost:27017" \
--db-type mongodb --azure-storage-connection-string "<storage_connection_string>"
```

## Data Storage

The data fetched from GitHub is stored in the specified database with the following collections or containers:

- `About`: Repository description.
- `TrafficStats`: Views and unique visitors over time.
- `GitClones`: Clones and unique cloners over time.
- `Stars`: Stars over time.
- `Forks`: Forks over time.

## Migration

The `migrate.py` script is used to migrate data from MongoDB to Azure Cosmos DB.

### Usage

```bash
python migrate.py
```

Before running the migration script, ensure you have updated the connection strings in `migrate.py`:

- `mongo_uri`: MongoDB connection string.
- `cosmos_uri`: Azure Cosmos DB connection string.

## Logging

The scripts use Python's logging module to provide detailed logs. Errors during migration are logged to `migration_errors.log`.

## Contributing

Contributions are welcome! Please open an issue or submit a pull request for any improvements or bug fixes.

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.

---

I hope this README meets your requirements. Let me know if there's anything you'd like to add or change.