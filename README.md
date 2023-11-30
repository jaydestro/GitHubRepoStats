# GitHub Repository Traffic Data Fetcher

This tool is designed to fetch and process traffic data for GitHub repositories. It uses the GitHub API to retrieve information about repository views, clones, referrers, and popular content. The data is then transformed, aggregated, and can be saved in Excel format and/or a MongoDB database.

## Features

- **Data Retrieval**: Fetches traffic, clones, referrers, and popular content data from GitHub repositories.
- **Timestamping**: Each record for referring sites and popular content includes a timestamp indicating the fetch time.
- **Data Aggregation**: Provides month-to-month and year-to-year aggregated data for easy analysis.
- **Excel Output**: Processed data can be saved to an Excel file with separate sheets for each data category.
- **MongoDB Integration**: Option to store the fetched data in a MongoDB database for persistent storage and retrieval.
- **Command Line Interface**: Easy to use CLI for specifying repository details and optional parameters.

## Prerequisites

- Python 3.x
- Pandas (`pip install pandas`)
- Requests (`pip install requests`)
- MongoDB Python Driver - PyMongo (`pip install pymongo`) (Optional for MongoDB integration)
- GitHub Personal Access Token (with appropriate permissions)

To install the required Python libraries, run:

```bash
pip install -r requirements.txt
```

## Usage
1. Ensure you have a GitHub Personal Access Token with appropriate permissions. [Learn how to create one here.](https://docs.github.com/en/authentication/keeping-your-account-and-data-secure/creating-a-personal-access-token)
1. Store your Personal Access Token in a text file for security.
1. Run the script using the command:

```bash
python script_name.py --repo <repository_name> --owner <owner_name> --token-file <path_to_token_file> [--filename <optional_filename.xlsx>] [--mongodb-connection-string <your_mongodb_connection_string>]
```

Replace `<repository_name>`, `<owner_name>`, `<optional_filename.xlsx>`, and `<path_to_token_file>` with your repository's name, repository owner's name, an optional filename for the Excel report, and the path to your token file, respectively.

## Configuration

The script accepts the following command-line arguments:

- `--repo`: The name of the GitHub repository.
- `--owner`: The GitHub username or organization name that owns the repository.
- `--filename` (optional): Specify the filename for the Excel report. If not provided, it defaults to `{owner}-{repo}-traffic-data.xlsx`.
- `--token-file`: The path to the text file containing your GitHub Personal Access Token.
- `--mongodb-connection-string` (optional): The MongoDB connection string to store the data. Omit this flag if you do not wish to use MongoDB.

## Data Output

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

Each category is available in a structured format and can be saved into separate sheets in an Excel file. If MongoDB integration is configured, each category is also saved as a collection in the database, allowing for persistent storage and advanced querying capabilities.

## Contributing

Contributions to this project are welcome! Please fork the repository and submit a pull request with your changes.
