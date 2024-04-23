import datetime
import logging
import os
import azure.functions as func
from report import retrieve_and_process_stats, read_file_from_azure_blob

app = func.FunctionApp()

@app.schedule(schedule="0 0 0 * * *", arg_name="myTimer", run_on_startup=True,
              use_monitor=False) 
def main(myTimer: func.TimerRequest) -> None:
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()

    if myTimer.past_due:
        logging.info('The timer is past due!')

    # Environment variables for database and storage connections
    mongodb_connection_string = os.environ["CUSTOMCONNSTR_MONGODB"]
    azure_storage_connection_string = os.environ["CUSTOMCONNSTR_BLOBSTORAGE"]
    token = os.environ["GHPAT"]
    output_format = os.environ["OUTPUT"]

    # Retrieve file and parse out owner/repos to collect stats
    try:
        repos = read_file_from_azure_blob(azure_storage_connection_string, "githubrepostats", "repos.csv")
        if repos:
            total_repos = len(repos)
            processed_count = 0

            for owner, repo in repos:
                try:
                    filename = f"{owner}-{repo}-traffic-data"
                    logging.info(f"Starting to process {owner}/{repo}")
                    retrieve_and_process_stats(owner, repo, filename, mongodb_connection_string, azure_storage_connection_string, output_format, token)
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
    except Exception as e:
        logging.error(f"Failed to read from Azure Blob or process data: {str(e)}")

