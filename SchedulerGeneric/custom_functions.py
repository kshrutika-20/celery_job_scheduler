import logging
from api_caller import trigger_api_call

def daily_report_job(trace_id: str):
    logging.info(f"[{trace_id}] Running custom logic for daily_report_job")
    return trigger_api_call(
        url="https://httpbin.org/post", method="POST", trace_id=trace_id,
        headers={'Content-Type': 'application/json', 'X-API-Key': 'your-secret-api-key'},
        json_data={'report_type': 'daily_summary', 'send_email': True}
    )

def frequent_health_check(trace_id: str):
    logging.info(f"[{trace_id}] Running custom logic for frequent_health_check")
    return trigger_api_call(url="https://httpbin.org/get", method="GET", trace_id=trace_id)

def data_ingestion_parent(trace_id: str):
    logging.info(f"[{trace_id}] Running custom logic for data_ingestion_parent")
    return trigger_api_call(
        url="https://httpbin.org/post", method="POST", trace_id=trace_id,
        json_data={"source": "live-stream"}
    )

def data_processing_child(trace_id: str):
    logging.info(f"[{trace_id}] Running custom logic for data_processing_child")
    return trigger_api_call(
        url="https://httpbin.org/post", method="POST", trace_id=trace_id,
        json_data={"status": "complete"}
    )

JOB_FUNCTIONS = {
    'daily_report_job': daily_report_job,
    'frequent_health_check': frequent_health_check,
    'data_ingestion_parent': data_ingestion_parent,
    'data_processing_child': data_processing_child
}
