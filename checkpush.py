### This script checks that you have rights to push to a list of repositories. It reads a CSV file with repository names and checks if you have push access to each repository. The script writes the repositories you don't have push access to a text file and the repositories you have push access to a CSV file (to be used by the Azure Function).

import csv
import requests

# Path to the CSV file with repository names
csv_file = '<repos_file_name>'
# Path to your GitHub personal access token file
token_file = '<token_file_name>'
# Output CSV file for repositories without push access
no_access_file = 'repos_without_push_access.txt'
# Output CSV file for repositories with push access
access_csv = 'repos_with_push_access.csv'

# Read the GitHub personal access token from file
with open(token_file, 'r') as file:
    token = file.read().strip()

# Function to check if you have push access to a repository
def check_push_access(owner, repo):
    url = f'https://api.github.com/repos/{owner}/{repo}'
    headers = {'Authorization': f'token {token}'}
    response = requests.get(url, headers=headers)
    response_data = response.json()

    # Check if push access is false and log the repo if it is
    if not response_data.get('permissions', {}).get('push', False):
        with open(no_access_file, 'a') as file:
            file.write(f'{owner}/{repo}\n')
    else:
        # If push access exists, write to the CSV file
        with open(access_csv, mode='a', newline='') as file:
            csv_writer = csv.writer(file)
            csv_writer.writerow([owner, repo])

# Main execution
if __name__ == '__main__':
    # Ensure the CSV file starts with headers
    with open(access_csv, mode='w', newline='') as file:
        csv_writer = csv.writer(file)
        csv_writer.writerow(['owner', 'repo'])

    try:
        with open(csv_file, newline='') as file:
            reader = csv.DictReader(file)
            for row in reader:
                check_push_access(row['owner'], row['repo'])
    except KeyError as e:
        print(f"Error: CSV header not found. Please ensure the CSV includes the headers exactly as 'owner' and 'repo'. Detailed error: {e}")

    print(f'Repositories without push access are listed in {no_access_file}')
    print(f'Repositories with push access are listed in {access_csv}')

