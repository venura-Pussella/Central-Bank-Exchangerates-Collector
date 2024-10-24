import datetime
from main import run_main
import azure.functions as func
from src import logger
from azure.core.exceptions import AzureError

def main(mytimer: func.TimerRequest) -> None:
    utc_timestamp = datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc).isoformat()
    
    if mytimer.past_due:
        logger.info('The timer is past due!')

    try:
        run_main()
        logger.info('Python timer trigger (central bank exchange rate collector) function ran successfully at %s', utc_timestamp)

    except AzureError as ae:
        logger.warning(f"Transient Azure error occurred (central bank exchange rate collector): {ae}")
        logger.info("Function will be retried due to transient Azure error")
        raise  # Allow retry for transient errors