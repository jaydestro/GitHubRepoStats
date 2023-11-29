# GitHub Repository Traffic Data Fetcher

This Python script fetches traffic data for a specified GitHub repository and compiles it into an Excel report. The report includes detailed statistics about repository views, clones, referring sites, and popular content, with daily data and aggregated monthly and yearly totals.

## Features

- Fetches and processes traffic data from GitHub's API.
- Compiles data into an Excel report with separate sheets for:
  - Traffic stats (views and unique visitors)
  - Git clones (clones and unique cloners)
  - Referring sites
  - Popular content
- Adds daily, monthly, and yearly aggregated totals.
- Appends new data to existing reports, avoiding data duplication.

## Requirements

- Python 3
- Pandas library
- Requests library
- Openpyxl library

To install the required Python libraries, run:

```bash
pip install pandas requests openpyxl
```

## Usage
1. Ensure you have a GitHub Personal Access Token with appropriate permissions. Learn how to create one here.
1. Store your Personal Access Token in a text file for security.
1. Run the script using the command:

```bash
python script_name.py --repo <repository_name> --owner <owner_name> --filename <optional_filename.xlsx> --token-file <path_to_token_file>
```

Replace `<repository_name>`, `<owner_name>`, `<optional_filename.xlsx>`, and `<path_to_token_file>` with your repository's name, repository owner's name, an optional filename for the Excel report, and the path to your token file, respectively.

## Configuration

The script accepts the following command-line arguments:

- `--repo`: The name of the GitHub repository.
- `--owner`: The GitHub username or organization name that owns the repository.
- `--filename` (optional): Specify the filename for the Excel report. If not provided, it defaults to `{owner}-{repo}-traffic-data.xlsx`.
- `--token-file`: The path to the text file containing your GitHub Personal Access Token.

## Contributing

Contributions to this project are welcome! Please fork the repository and submit a pull request with your changes.
