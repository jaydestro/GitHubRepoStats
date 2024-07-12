# GitHub Repository Traffic Data Fetcher

This tool is designed to fetch and process traffic data for GitHub repositories. It uses the GitHub API to retrieve information about repository views, clones, referrers, and popular content. The data is stored in MongoDB or Azure Cosmos DB, and optionally uploaded to Azure Blob Storage for analysis and reporting.

## Table of Contents

1. [Motivation Behind the Creation](#motivation-behind-the-creation)
1. [Features](#features)
1. [Prerequisites](#prerequisites)
1. [Configuration](#configuration)
1. [Usage](#usage)
1. [Data Output](#data-output)
1. [Azure Function Integration](#azfunc)
1. [Todo](#todo)
1. [Contributing](#contributing)
1. [License](#license)

## Motivation Behind the Creation

The primary motivation for creating this script was to overcome a significant limitation of GitHub's native traffic data functionality, which only provides insights for the past 14 days. This restricted timeframe can be limiting for long-term analysis and understanding of the repository's traffic trends.

To address this issue, this script was developed to fetch and store GitHub traffic data in a database, enabling the tracking of historical data over extended periods. By archiving this data, users can perform more comprehensive analyses, identify trends, and make informed decisions based on a much richer dataset that spans beyond the 14-day window offered by GitHub.

## Features

- **Data Retrieval**: Fetches traffic, clones, referrers, and popular content data from GitHub repositories.
- **Timestamping**: Each record for referring sites and popular content includes a timestamp indicating the fetch time.
- **Data Aggregation**: Provides month-to-month and year-to-year aggregated data for easy analysis.
- **Excel/JSON Output**: Processed data can be saved to an Excel file with separate sheets for each data category or a JSON file for each collection within the database.
- **MongoDB and Cosmos DB Integration**: Option to store the fetched data in MongoDB or Azure Cosmos DB for persistent storage and retrieval.
- **Azure Blob Storage**: Option to store the Excel report in Azure Blob Storage, with the ability to generate a 24-hour temporary URL for secure file sharing.
- **Command Line Interface**: Easy to use CLI for specifying repository details and optional parameters.

## Prerequisites

- Python 3.7 or higher
- GitHub Personal Access Token (with appropriate permissions)
- MongoDB or Azure Cosmos DB connection string
- (Optional) Azure Blob Storage connection string

> **Note:** You can sign up for Azure Cosmos DB for MongoDB for free and start building scalable applications. To get started, visit [Azure Cosmos DB](https://aka.ms/trycosmosdb) to create a free account and explore the powerful features of Azure Cosmos DB.

## Getting Started

1. **Clone the repository**:

    ```sh
    git clone <repository-url>
    cd <repository-directory>
    ```

2. **Create a virtual environment**:

    ```sh
    python -m venv venv
    source venv/bin/activate  # On Windows use `venv\\Scripts\\activate`
    ```

3. **Install dependencies**:

    ```sh
    pip install -r requirements.txt
    ```

### GitHub Codespaces (Optional)

You can try out this implementation by running the code in [GitHub Codespaces](https://docs.github.com/codespaces/overview) instead of a local clone.

- Open the application code in a GitHub Codespace:

    [![Open in GitHub Codespaces](https://github.com/codespaces/badge.svg)](https://codespaces.new/jaydestro/GitHubRepoStats?quickstart=1&devcontainer_path=.devcontainer%2Fdevcontainer.json)

## Configuration

The script accepts the following command-line arguments:

- `--repo`: The name of the GitHub repository.
- `--owner`: The GitHub username or organization name that owns the repository.
- `--filename` (optional): Specify the filename for the Excel report. If not provided, it defaults to `{owner}-{repo}-traffic-data.xlsx`.
- `--token-file`: The path to the text file containing your GitHub Personal Access Token.
- `--db-connection-string`: The database connection string to store and retrieve the data.
- `--db-type`: Type of database (`mongodb` or `cosmosdb`).
- `--azure-storage-connection-string` (optional): The Azure Blob Storage connection string for storing the Excel file. Omit this flag if you do not want to use Azure Blob Storage.
- `--output-format` (optional): Specify the output format for the data (`excel`, `json`, `all`). Defaults to `excel`.

## Usage

1. Ensure you have a GitHub Personal Access Token with appropriate permissions. [Learn how to create one here](https://docs.github.com/en/authentication/keeping-your-account-and-data-secure/creating-a-personal-access-token).
2. Store your Personal Access Token in a text file for security.
3. Run the script using the command:

    ```sh
    python report.py --repo <repository_name> --owner <owner_name> --token-file <path_to_token_file> --db-connection-string <db_connection_string> --db-type <db_type> [--filename <optional_filename>] [--output-format <excel/json/all>] [--azure-storage-connection-string <your_azure_blob_storage_connection_string>]
    ```

Replace `<repository_name>`, `<owner_name>`, `<path_to_token_file>`, `<db_connection_string>`, `<db_type>`, `<optional_filename>`, and `<your_azure_blob_storage_connection_string>` with your repository's name, repository owner's name, the path to your token file, your database connection string, the database type (`mongodb` or `cosmosdb`), and your optional storage connection string, respectively.

## Data Output

![Example Traffic Stats Excel Sheet](./images/example_traffic_sheet.jpg)

*Example of an Excel sheet generated by the GitHub Repository Traffic Data Fetcher. This sheet illustrates traffic statistics including views, unique visitors, clones, and referring sites. It provides a comprehensive view of the repository's engagement over time.*

The script processes and outputs data in the following categories:

1. **Traffic Stats**:
   - Shows views and unique visitors statistics of the repository.
   - Data includes total views, unique visitors, and timestamps for each record.
   - Aggregated monthly and yearly totals are also provided for comprehensive analysis.

2. **Git Clones**:
   - Provides clone counts and unique cloners of the repository.
   - Each record includes the total number of clones and unique cloners, along with the corresponding date.
   - Like traffic stats, this category also includes month-to-month and year-to-year aggregated data.

3. **Referring Sites**:
   - Lists top referring sites with their views and unique visitors.
   - Each entry is timestamped to indicate the exact time of data retrieval.
   - The data is aggregated monthly and yearly to observe trends and changes over time.

4. **Popular Content**:
   - Highlights the most visited paths in the repository.
   - Records include path details, view counts, unique visitors, and a timestamp.
   - Aggregated data on a monthly and yearly basis is provided for in-depth analysis.

5. **Stars and Forks**:
   - Tracks the number of stars and forks the repository receives over time.
   - Each star or fork is recorded with its respective date, allowing for a detailed view of the repository's growing popularity and community engagement.
   - Data is aggregated to show monthly and yearly trends, providing insights into the repository's long-term appeal and reach.

Each category is available in a structured format and is saved into separate sheets in an Excel file. Additionally, each category is stored as a collection in the MongoDB or Azure Cosmos DB database, providing persistent storage and enabling advanced querying capabilities. The option to store the Excel file in Azure Blob Storage adds another layer of functionality, allowing for secure and convenient sharing of the report.

## Azure Function Integration

This tool can be integrated with Azure Functions, enabling it to run as a time-triggered function. This allows for automated, scheduled fetching of GitHub repository traffic data without manual intervention.

### Key Benefits

- **Automated Scheduling**: Configure the function to run at specific times (e.g., daily at midnight) to regularly update your traffic data.
- **Scalability**: Leverage Azure's serverless architecture to handle varying loads without the need to manage server resources.
- **Reliability**: Azure Functions ensures high availability and consistent performance, ensuring that your data fetching process runs smoothly.
- **Cost-Effective**: Pay only for the compute resources you use while the Azure Function is running, leading to potential cost savings compared to always-on solutions.

### Setup

To set up the Azure Function:

1. **Deploy the provided `function_app.py` script in your Azure Functions environment.**

    ```python
    import datetime
    import logging
    import os
    import azure.functions as func
    from report import retrieve_and_process_stats, read_file_from_azure_blob

    app = func.FunctionApp()

    @app.schedule(schedule="0 0 0 * * *", arg_name="myTimer", run_on_startup=True, use_monitor=False) 
    def main(myTimer: func.TimerRequest) -> None:
        utc_timestamp = datetime.datetime.utcnow().replace(
            tzinfo=datetime.timezone.utc).isoformat()

        if myTimer.past_due:
            logging.info('The timer is past due!')

        try:
            # Environment variables for database and storage connections
            mongodb_connection_string = os.environ["CUSTOMCONNSTR_MONGODB"]
            azure_storage_connection_string = os.environ["CUSTOMCONNSTR_BLOBSTORAGE"]
            token = os.environ["GHPAT"]
            output_format = os.environ["OUTPUT"]
            use_managed_identity = os.environ.get("USE_MANAGED_IDENTITY", "false").lower() == "true"

            # Retrieve file and parse out owner/repos to collect stats
            repos = read_file_from_azure_blob(azure_storage_connection_string, "githubrepostats", "repos.csv", use_managed_identity)
            if repos:
                total_repos = len(repos)
                processed_count = 0

                for owner, repo in repos:
                    try:
                        filename = f"{owner}-{repo}-traffic-data"
                        logging.info(f"Starting to process {owner}/{repo}")
                        retrieve_and_process_stats(owner, repo, filename, mongodb_connection_string, azure_storage_connection_string, output_format, token, use_managed_identity)
                        processed_count += 1
                        logging.info(f"Successfully processed {owner}/{repo}")
                    except Exception as e:
                        logging.error(f"Error processing {owner}/{repo}: {str(e)}")

                if processed_count == total_repos:
                    logging.info("Successfully processed all repositories.")
                else:
                    logging.warning(f"Processed {processed_count} out of {total_repos} repositories. Some repositories failed to process.")

            else:
                logging.warning("No repositories found to process.")
        except KeyError as e:
            logging.error(f"Missing environment variable: {str(e)}")
        except Exception as e:
            logging.error(f"Failed to read from Azure Blob or process data: {str(e)}")
    ```

2. **Configure the function trigger using the `host.json` file to specify the execution schedule.**

    ```json
    {
        "version": "2.0",
        "extensions": {
            "timers": {
                "schedule": "0 0 0 * * *"  // Runs daily at midnight, adjust as needed
            }
        }
    }
    ```

3. **Set the necessary environment variables in your Azure Function application settings.** These variables are crucial for the script to access required resources and perform its operations:

   - `OWNER`: The GitHub username or organization name that owns the repository.
   - `REPO`: The name of the GitHub repository.
   - `GHPAT`: Your GitHub Personal Access Token.
   - `OUTPUT`: The desired output format for the processed data (e.g., `excel`, `json`, etc.).
   - `CUSTOMCONNSTR_MONGODB`: The MongoDB connection string for storing fetched data.
   - `CUSTOMCONNSTR_BLOBSTORAGE`: The Azure Blob Storage connection string for storing the Excel report.

Once configured, the Azure Function will automatically trigger at the scheduled times. It will fetch the latest GitHub repository traffic data and process it as configured, storing the results in MongoDB and optionally in Azure Blob Storage. The function's behavior and data handling are controlled by the set environment variables, ensuring a flexible and customizable operation.

## Todo

This project has several areas for future development and improvement to enhance its capabilities and integration with other services. The following items are on the current to-do list:

1. **Create tests**:
   - Use `pytest` to validate that the script executes.
   - Mock GitHub output data
   - Mock MongoDB server to ensure that queries and upserts work.
   - Have this run via GitHub action.

## Contributing

Contributions to this project are welcome! Please fork the repository and submit a pull request with your changes.

## License

This script is released under the [MIT License](https://opensource.org/licenses/MIT). Feel free to use, modify, and distribute the code in accordance with the terms specified in the license.
