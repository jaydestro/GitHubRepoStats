import datetime
import subprocess
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
# retrieve file and parse out owner/repos to collect stats
# loop here
    
    mongodb_connection_string = os.environ["CUSTOMCONNSTR_MONGODB"]
    azure_storage_connection_string = os.environ["CUSTOMCONNSTR_BLOBSTORAGE"]
    
    token = os.environ["GHPAT"]
    output_format = os.environ["OUTPUT"]

    repos = read_file_from_azure_blob(azure_storage_connection_string, "githubrepostats", "repos.csv")
    for owner, repo in repos:
        filename = None
        print("Starting to process {owner}/{repo}")
        retrieve_and_process_stats(owner, repo, filename, mongodb_connection_string, azure_storage_connection_string, output_format, token)
