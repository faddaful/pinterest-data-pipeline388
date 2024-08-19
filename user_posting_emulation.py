# Import the necessary modules and libraries
import json
import random
from time import sleep
import requests
import yaml
import sqlalchemy
from sqlalchemy.sql import text
import boto3
import pymysql
from datetime import datetime

# Script class
class AWSDBConnector:
    """
    A class to handle the connection to the AWS RDS database using SQLAlchemy.
    """
    def __init__(self, db_creds):
        """
        Initializes the AWSDBConnector with database credentials file.
        
        :param db_creds: Path to the YAML file containing database credentials.
        """
        self.db_creds = db_creds

    def read_db_creds(self):
        """
        Reads the database credentials from the YAML file.
        
        :return: A dictionary containing the database credentials.
        """
        with open(self.db_creds, 'r') as file:
            creds = yaml.safe_load(file)
        return creds
        
    def create_db_connector(self):
        """
        Creates a SQLAlchemy engine using the database credentials.
        
        :return: A SQLAlchemy engine object.
        """
        creds = self.read_db_creds()
        engine = sqlalchemy.create_engine(
            f"mysql+pymysql://{creds['USER']}:{creds['PASSWORD']}@{creds['HOST']}:{creds['PORT']}/{creds['DATABASE']}?charset=utf8mb4"
        )
        return engine

# Initialize the database connector
new_connector = AWSDBConnector("db_creds.yaml")
print(new_connector)

def fetch_data_from_db(random_row):
    """
    Fetches data from pinterest_data, geolocation_data, and user_data tables for a given row.
    
    :param random_row: Row number to fetch data from.
    :return: A tuple containing pinterest_data, geolocation_data, and user_data.
    """
    engine = new_connector.create_db_connector()
    with engine.connect() as connection:
        pin_result = {}
        geo_result = {}
        user_result = {}

        # Fetch pinterest data
        pin_string = text(f"SELECT * FROM pinterest_data LIMIT {random_row}, 1")
        pin_selected_row = connection.execute(pin_string)
        for row in pin_selected_row:
            pin_result = dict(row._mapping)
        
        # Fetch geolocation data
        geo_string = text(f"SELECT * FROM geolocation_data LIMIT {random_row}, 1")
        geo_selected_row = connection.execute(geo_string)
        for row in geo_selected_row:
            geo_result = dict(row._mapping)
        
        # Fetch user data
        user_string = text(f"SELECT * FROM user_data LIMIT {random_row}, 1")
        user_selected_row = connection.execute(user_string)
        for row in user_selected_row:
            user_result = dict(row._mapping)
    
    return pin_result, geo_result, user_result

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
    
    :param pin_result: Data from pinterest_data table.
    :param geo_result: Data from geolocation_data table.
    :param user_result: Data from user_data table.
    """
    base_url = "https://sjpa7o86pd.execute-api.us-east-1.amazonaws.com/prod"
    invoke_urls = {
        "pin": f"{base_url}/topics/0affe012670f.pin",
        "geo": f"{base_url}/topics/0affe012670f.geo",
        "user": f"{base_url}/topics/0affe012670f.user"
    }
    headers = {'Content-Type': 'application/vnd.kafka.json.v2+json'}
    # Use a try except method to catch any expected and unexpected error
    try:
        for topic, result in zip(["pin", "geo", "user"], [pin_result, geo_result, user_result]):
            
            payload = {"records": [{"value": result}]}

            # Serialize the payload with custom DateTimeEncoder
            json_payload = json.dumps(payload, cls=DateTimeEncoder)

            #Print the payload for confirmation
            print("This is the payload dump: ", json_payload)

            response = requests.request("POST", invoke_urls[topic], headers=headers, data=json_payload)

            # Print response status code for validation purpose and debugging
            print(f"Posted to {topic} with response: {response.status_code}")
            if response.status_code != 200:
                # Print error if not 200
                print(f"Error posting to {topic}: {response.text}")
    except Exception as e:
        print(f"Exception occurred during posting to API: {e}")


def run_infinite_post_data_loop():
    """
    Runs an infinite loop that periodically fetches random rows from database tables
    and posts the data to corresponding API endpoints. Also, uploads the data to S3.
    """
    while True:
        # Sleep for a random interval between 0 and 2 seconds
        sleep(random.randrange(0, 2))
        random_row = random.randint(0, 11000)

        # Fetch data from database
        pin_result, geo_result, user_result = fetch_data_from_db(random_row)

        # If the posting is successful, break the loop
        try:
            # Attempt to post data to API
            post_data_to_api(pin_result, geo_result, user_result)
            print("Data posted successfully!")
            break  # Break the loop if no exception is raised

        except Exception as e:
            # Handle the exception if posting fails
            print(f"Failed to post data: {e}. Retrying...")
    

if __name__ == "__main__":

    """
    The ec2 client and kafka connector need to be started first.
    Run Post_data_to_api function only to post data once
    or
    Run the infinite loop to post data continuously.
    """
    # Sleep for a random interval between 0 and 2 seconds
    #sleep(random.randrange(0, 2))
    #random_row = random.randint(0, 11000)

    # Fetch data from database
    #pin_result, geo_result, user_result = fetch_data_from_db(random_row)
    # Post data to API
    # post_data_to_api(pin_result, geo_result, user_result)

    # run the infinite loop
    run_infinite_post_data_loop()
    #print("Infinite loop is currently running")
    #print("This is the pin file :", pin_result)
    