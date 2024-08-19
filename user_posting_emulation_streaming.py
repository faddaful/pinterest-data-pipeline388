# Import the necessary modules and libraries
import requests
import random
import json
from datetime import datetime
from time import sleep
from user_posting_emulation import fetch_data_from_db

# Class to encode the json data
class DateTimeEncoder(json.JSONEncoder):
    """
    Custom JSON encoder to handle datetime objects.
    """
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super(DateTimeEncoder, self).default(obj)
# Post to API function
def post_data_to_api(pin_result, geo_result, user_result):
    """
    Posts data to the corresponding API endpoints.
    
    :param pin_results: List of data from pinterest_data table.
    :param geo_results: List of data from geolocation_data table.
    :param user_results: List of data from user_data table.
    """
    # This invoke url post data to the api just once
    invoke_url_pin = "https://sjpa7o86pd.execute-api.us-east-1.amazonaws.com/prods/streams/streaming-0affe012670f-pin/record"
    headers = {'Content-Type': 'application/json'}
    
    # Use try except to catch expected and unexpected errors
    try:
        payload = json.dumps({
            "StreamName": "streaming-0affe012670f-pin", 
            "Data": pin_result, 
                 "PartitionKey": "partition-1"}, cls=DateTimeEncoder)
        # Print the payload for confirmation
        print(f"Payload for PIN Data dump: {payload}")

        response = requests.put(invoke_url_pin, headers=headers, data=payload)
        print(f"Print Reponse at put request stage PIN: ",response.json())
        # Print the status code after posting to the API
        print(f"Posted to PIN stream with response: {response.status_code}")

        if response.status_code != 200:
            print(f"Error posting to PIN stream: {response.text}")
    except Exception as e:
        print(f"Exception occurred during posting PIN to API: {e}")



    invoke_url_geo = "https://sjpa7o86pd.execute-api.us-east-1.amazonaws.com/prods/streams/streaming-0affe012670f-geo/record"
    headers = {'Content-Type': 'application/json'}
    # Use try except to catch expected and unexpected errors
    try:
        payload = json.dumps({
            "StreamName": "streaming-0affe012670f-geo", 
            "Data": geo_result, 
                 "PartitionKey": "partition-1"}, cls=DateTimeEncoder)
        # Print the payload for confirmation
        print(f"Payload for GEO Data dump: {payload}")

        response = requests.put(invoke_url_geo, headers=headers, data=payload)
        print(f"Print Reponse at put request stage GEO: ",response.json())
        
        # Print the status code after posting to the API
        print(f"Posted to GEO stream with response: {response.status_code}")

        if response.status_code != 200:
            print(f"Error posting to GEO stream: {response.text}")
    except Exception as e:
        print(f"Exception occurred during posting GEO to API: {e}")

    
    invoke_url_user = "https://sjpa7o86pd.execute-api.us-east-1.amazonaws.com/prods/streams/streaming-0affe012670f-user/record"
    headers = {'Content-Type': 'application/json'}

    try:
        payload = json.dumps({
            "StreamName": "streaming-0affe012670f-user", 
            "Data": user_result, 
                 "PartitionKey": "partition-1"}, cls=DateTimeEncoder)
        
        print(f"Payload for USER Data dump: {payload}")

        response = requests.put(invoke_url_user, headers=headers, data=payload)
        print(f"Print Reponse at put request stage USER: ",response.json())

        print(f"Posted to USER stream with response: {response.status_code}")

        if response.status_code != 200:
            print(f"Error posting to USER stream: {response.text}")
    except Exception as e:
        print(f"Exception occurred during posting USER to API: {e}")

# Usage
if __name__ == "__main__":
    # Run a while loop to post data to the API consistently.
    while True:

        # Sleep for a random interval between 0 and 2 seconds
        sleep(random.randrange(0, 2))
        random_row = random.randint(0, 11000)

        # Fetch data from database
        pin_result, geo_result, user_result = fetch_data_from_db(random_row)
        # Post data to API
        post_data_to_api(pin_result, geo_result, user_result)