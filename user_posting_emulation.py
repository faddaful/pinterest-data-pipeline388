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

class AWSDBConnector:
    """
    A class to handle the connection to an AWS RDS MySQL database using SQLAlchemy.
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

    try:
        for topic, result in zip(["pin", "geo", "user"], [pin_result, geo_result, user_result]):
            payload = {"records": [{"value": result}]}
            response = requests.post(invoke_urls[topic], headers=headers, json=payload)
            print(f"Posted to {topic} with response: {response.status_code}")
            if response.status_code != 200:
                print(f"Error posting to {topic}: {response.text}")
    except Exception as e:
        print(f"Exception occurred: {e}")

def upload_data_to_s3(random_row, pin_result, geo_result, user_result):
    """
    Uploads data to AWS S3.
    
    :param random_row: Row number to use in the S3 object key.
    :param pin_result: Data from pinterest_data table.
    :param geo_result: Data from geolocation_data table.
    :param user_result: Data from user_data table.
    """
    s3_client = boto3.client('s3')
    bucket_name = 'user-0affe012670f-bucket'

    try:
        s3_client.put_object(
            Bucket=bucket_name,
            Key=f'pinterest_data/{random_row}.json',
            Body=json.dumps(pin_result, default=str)
        )
        s3_client.put_object(
            Bucket=bucket_name,
            Key=f'geolocation_data/{random_row}.json',
            Body=json.dumps(geo_result, default=str)
        )
        s3_client.put_object(
            Bucket=bucket_name,
            Key=f'user_data/{random_row}.json',
            Body=json.dumps(user_result, default=str)
        )
        print("Data uploaded to S3")
    except Exception as e:
        print(f"Exception occurred during S3 upload: {e}")

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

        # Post data to API
        post_data_to_api(pin_result, geo_result, user_result)

        # Upload data to S3
        upload_data_to_s3(random_row, pin_result, geo_result, user_result)

if __name__ == "__main__":

    # To debug, you can run each function individually here
    # For example:
    random_row = random.randint(0, 11000)
    pin_result, geo_result, user_result = fetch_data_from_db(random_row)
    print(pin_result, geo_result, user_result)
    
    post_data_to_api(pin_result, geo_result, user_result)
    
    upload_data_to_s3(random_row, pin_result, geo_result, user_result)

    # Or run the infinite loop (uncomment to use)
    # run_infinite_post_data_loop()
    print('Working')



















# random.seed(100)

# class AWSDBConnector:

#     def __init__(self, db_creds):
#         self.db_creds = db_creds

#     def read_db_creds(self):
#         with open(self.db_creds, 'r') as file:
#             creds = yaml.safe_load(file)
#         return creds
        
#     def create_db_connector(self):
#         creds = self.read_db_creds()
#         engine = sqlalchemy.create_engine(f"mysql+pymysql://{creds['USER']}:{creds['PASSWORD']}@{creds['HOST']}:{creds['PORT']}/{creds['DATABASE']}?charset=utf8mb4")
#         return engine


# new_connector = AWSDBConnector("db_creds.yaml")
# print(new_connector)


# def run_infinite_post_data_loop():
#     while True:
#         sleep(random.randrange(0, 2))
#         random_row = random.randint(0, 11000)
#         engine = new_connector.create_db_connector()

#         with engine.connect() as connection:

#             pin_string = text(f"SELECT * FROM pinterest_data LIMIT {random_row}, 1")
#             pin_selected_row = connection.execute(pin_string)
            
#             for row in pin_selected_row:
#                 pin_result = dict(row._mapping)

#             geo_string = text(f"SELECT * FROM geolocation_data LIMIT {random_row}, 1")
#             geo_selected_row = connection.execute(geo_string)
            
#             for row in geo_selected_row:
#                 geo_result = dict(row._mapping)

#             user_string = text(f"SELECT * FROM user_data LIMIT {random_row}, 1")
#             user_selected_row = connection.execute(user_string)
            
#             for row in user_selected_row:
#                 user_result = dict(row._mapping)
            
#             print(pin_result)
#             print(geo_result)
#             print(user_result)


# if __name__ == "__main__":
   
#     run_infinite_post_data_loop()
#     print('Working')
    

#      # Create the invoke url per topic
#     invoke_url = "https://sjpa7o86pd.execute-api.us-east-1.amazonaws.com/prod/topics/0affe012670f.pin"
#     invoke_url = "https://sjpa7o86pd.execute-api.us-east-1.amazonaws.com/prod/topics/0affe012670f.geo"
#     invoke_url = "https://sjpa7o86pd.execute-api.us-east-1.amazonaws.com/prod/topics/0affe012670f.user"

#     headers = {'Content-Type': 'application/vnd.kafka.json.v2+json'}
#     response = requests.request("POST", invoke_url, headers=headers, data=payload)
    


