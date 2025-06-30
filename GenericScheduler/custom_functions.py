import logging
from api_caller import trigger_api_call

# Custom function to generate a daily report by making a POST API call
def daily_report_job():
    logging.info("Running custom logic for daily_report_job")
    # Add preprocessing logic here (e.g., fetch data)
    return trigger_api_call(
        url="http://127.0.0.1:5000/v1/reports/generate",
        method="POST",
        headers={
            'Content-Type': 'application/json',
            'X-API-Key': 'your-secret-api-key'
        },
        json_data={
            'report_type': 'daily_summary',
            'send_email': True
        }
    )

# Custom function to check API health

def frequent_health_check():
    logging.info("Running custom logic for frequent_health_check")
    return trigger_api_call(url="http://127.0.0.1:5000/health", method="GET")

# Function to initiate data ingestion process

def data_ingestion_parent():
    logging.info("Running custom logic for data_ingestion_parent")
    return trigger_api_call(
        url="http://127.0.0.1:5000/ingestion/start",
        method="POST",
        json_data={"source": "live-stream"}
    )

# Function to process data after ingestion completes

def data_processing_child():
    logging.info("Running custom logic for data_processing_child")
    # Imagine this fetches something before posting
    return trigger_api_call(
        url="http://127.0.0.1:5000/ingestion/process",
        method="POST",
        json_data={"status": "complete"}
    )

# Function mapping for dynamic dispatching based on job_id
JOB_FUNCTIONS = {
    'daily_report_job': daily_report_job,
    'frequent_health_check': frequent_health_check,
    'data_ingestion_parent': data_ingestion_parent,
    'data_processing_child': data_processing_child
}
