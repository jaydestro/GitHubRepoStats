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

    mongodb_connection_string = os.getenv("CUSTOMCONNSTR_MONGODB")
    azure_storage_connection_string = os.getenv("CUSTOMCONNSTR_BLOBSTORAGE")
    token = os.getenv("GHPAT")
    output_format = os.getenv("OUTPUT")

    try:
        repos = read_file_from_azure_blob(azure_storage_connection_string, "githubrepostats", "repos.csv")
        for owner, repo in repos:
            logging.info(f"Starting to process {owner}/{repo}")
            retrieve_and_process_stats(owner, repo, None, mongodb_connection_string, azure_storage_connection_string, output_format, token)
    except Exception as e:
        logging.error(f"Failed to process repositories: {str(e)}")