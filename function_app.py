import datetime
import subprocess
import logging
import os
import azure.functions as func
from report import retrieve_and_process_stats

app = func.FunctionApp()

@app.schedule(schedule="0 0 0 * * *", arg_name="myTimer", run_on_startup=True,
              use_monitor=False) 
def main(myTimer: func.TimerRequest) -> None:
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()

    if myTimer.past_due:
        logging.info('The timer is past due!')

    owner = os.environ["OWNER"]
    repo = os.environ["REPO"]
    token = os.environ["GHPAT"]
    output_format = os.environ["OUTPUT"]
    filename = None

    mongodb_connection_string = os.environ["CUSTOMCONNSTR_MONGODB"]
    azure_storage_connection_string = os.environ["CUSTOMCONNSTR_BLOBSTORAGE"]
    retrieve_and_process_stats(owner, repo, filename, mongodb_connection_string, azure_storage_connection_string, output_format, token)
