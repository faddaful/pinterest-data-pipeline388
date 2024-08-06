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

def post_data_to_api(pin_results, geo_results, user_results):
    """
    Posts data to the corresponding API endpoints.
    
    :param pin_results: List of data from pinterest_data table.
    :param geo_results: List of data from geolocation_data table.
    :param user_results: List of data from user_data table.
    """
    base_url = "https://sjpa7o86pd.execute-api.us-east-1.amazonaws.com/prods/streams/{stream_name}/records"
    stream_names = {
        "pin": "streaming-0affe012670f-pin",
        "geo": "streaming-0affe012670f-geo",
        "user": "streaming-0affe012670f-user"
    }
    headers = {'Content-Type': 'application/json'}

    try:
        for topic, results in zip(["pin", "geo", "user"], [pin_results, geo_results, user_results]):
            records = []
            for result in results:
                # Convert the dictionary to a JSON string
                records.append({
                    "Data": result,
                    #"PartitionKey": f"partition-1"
                })
            
            payload = json.dumps({
                "StreamName": stream_names[topic],
                "Data": records, "PartitionKey": f"partition-1"
            }, cls=DateTimeEncoder)
            
            # Replacing the placeholder with the actual stream name
            invoke_url = base_url.replace("{stream_name}", stream_names[topic])
            
            # Print the payload for confirmation
            print(f"This is the payload dump for {topic}: ", payload)

            response = requests.put(invoke_url, headers=headers, data=payload)
            print(response.json())

            print(f"Posted to {topic} with response: {response.status_code}")
            if response.status_code != 200:
                print(f"Error posting to {topic}: {response.text}")
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
