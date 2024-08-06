import requests
import random
import json
from datetime import datetime
from time import sleep
from user_posting_emulation import fetch_data_from_db

class DateTimeEncoder(json.JSONEncoder):
    """
    Custom JSON encoder to handle datetime objects.
    """
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super(DateTimeEncoder, self).default(obj)

def post_data_to_api(pin_result, geo_result, user_result):
    """
    Posts data to the corresponding API endpoints.
    
    :param pin_results: List of data from pinterest_data table.
    :param geo_results: List of data from geolocation_data table.
    :param user_results: List of data from user_data table.
    """
    invoke_url = "https://sjpa7o86pd.execute-api.us-east-1.amazonaws.com/prods/streams/streaming-0affe012670f-pin/record"
    headers = {'Content-Type': 'application/json'}

    try:
        payload = json.dumps({
            "StreamName": "streaming-0affe012670f-pin", 
            "Data": pin_result, 
                 "PartitionKey": "partition-1"}, cls=DateTimeEncoder)
        
        print(f"Payload for Data dump: {payload}")

        response = requests.put(invoke_url, headers=headers, data=payload)
        print(f"Print Reponse at put request stage: ",response.json())

        print(f"Posted to stream with response: {response.status_code}")

        if response.status_code != 200:
            print(f"Error posting to stream: {response.text}")
    except Exception as e:
        print(f"Exception occurred during posting to API: {e}")

# Usage
if __name__ == "__main__":
    # Sleep for a random interval between 0 and 2 seconds
    sleep(random.randrange(0, 2))
    random_row = random.randint(0, 11000)

    # Fetch data from database
    pin_result, geo_result, user_result = fetch_data_from_db(random_row)
    # Post data to API
    post_data_to_api(pin_result, geo_result, user_result)