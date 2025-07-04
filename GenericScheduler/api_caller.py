import requests
import logging


def trigger_api_call(url: str, method: str, trace_id: str, headers: dict = None, json_data: dict = None) -> bool:
    """
    Triggers an API call, injecting a trace_id into the headers.
    Args:
            url (str): aThe URL of the API endpoint.
            method (str): The HTTP method (e.g., 'GET', 'POST').
            headers (dict, optional): A dictionary of HTTP headers. Defaults to None.
            json_data (dict, optional): A dictionary for the JSON payload (for POST/PUT). Defaults to None.
            trace_id (str): The trace ID to trigger.

    Returns:
            bool: True if the API call was successful (status code 2xx), False otherwise.
    """
    logging.info(f"[{trace_id}] Triggering API call: {method} {url}")

    request_headers = headers.copy() if headers else {}
    request_headers['X-Trace-ID'] = trace_id

    try:
        response = requests.request(
            method=method.upper(), url=url, headers=request_headers, json=json_data, timeout=30
        )
        response.raise_for_status()
        logging.info(f"[{trace_id}] API call successful for {url}. Status: {response.status_code}")
        return True
    except requests.exceptions.RequestException as e:
        logging.error(f"[{trace_id}] API call failed for {url}. Error: {e}")
        return False
