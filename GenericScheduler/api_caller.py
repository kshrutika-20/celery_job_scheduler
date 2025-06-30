import requests
import logging

# Utility to make API calls with error handling and logging

def trigger_api_call(url: str, method: str, headers: dict = None, json_data: dict = None) -> bool:
    """
        Triggers an API call using the requests library.

        Args:
            url (str): The URL of the API endpoint.
            method (str): The HTTP method (e.g., 'GET', 'POST').
            headers (dict, optional): A dictionary of HTTP headers. Defaults to None.
            json_data (dict, optional): A dictionary for the JSON payload (for POST/PUT). Defaults to None.

        Returns:
            bool: True if the API call was successful (status code 2xx), False otherwise.
        """
    logging.info(f"Triggering API call: {method} {url}")
    try:
        response = requests.request(
            method=method.upper(),
            url=url,
            headers=headers,
            json=json_data,
            timeout=30
        )
        response.raise_for_status()
        logging.info(f"API call successful for {url}. Status: {response.status_code}")
        return True
    except requests.exceptions.RequestException as e:
        logging.error(f"API call failed for {url}. Error: {e}")
        return False