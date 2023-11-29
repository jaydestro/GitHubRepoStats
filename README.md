# GitHub Repository Traffic Data Fetcher

This Python script fetches traffic data for a specified GitHub repository and compiles it into an Excel report. Additionally, it can optionally save the data to a MongoDB database. The report includes detailed statistics about repository views, clones, referring sites, and popular content, with daily data as well as aggregated monthly and yearly totals.

## Features

- Fetches and processes traffic data from GitHub's API.
- Compiles data into an Excel report with separate sheets for:
  - Traffic stats (views and unique visitors)
  - Git clones (clones and unique cloners)
  - Referring sites
  - Popular content
- Adds daily, monthly, and yearly aggregated totals.
- Optionally saves data to a MongoDB database.
- Appends new data to existing reports and database collections, avoiding data duplication.

## Requirements

- Python 3
- Pandas library
- Requests library
- Openpyxl library
- Pymongo library (for MongoDB functionality)

To install the required Python libraries, run:

```bash
pip install pandas requests openpyxl pymongo
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

## Contributing

Contributions to this project are welcome! Please fork the repository and submit a pull request with your changes.
