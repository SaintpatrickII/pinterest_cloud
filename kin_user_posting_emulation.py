import requests
from time import sleep
import random
from multiprocessing import Process
import boto3
import json
import sqlalchemy
from sqlalchemy import text
from decouple import config


random.seed(100)


HOST = config('HOST')
USER = config('USER')
PASSWORD = config('PASSWORD')
DATABASE = config('DATABASE')
PORT = config('PORT')



class AWSDBConnector:

    def __init__(self):

        self.HOST = HOST
        self.USER = USER
        self.PASSWORD = PASSWORD
        self.DATABASE = DATABASE
        self.PORT = PORT
        
    def create_db_connector(self):
        engine = sqlalchemy.create_engine(f"mysql+pymysql://{self.USER}:{self.PASSWORD}@{self.HOST}:{self.PORT}/{self.DATABASE}?charset=utf8mb4")
        return engine


new_connector = AWSDBConnector()


def run_infinite_post_data_loop():
    kin_invoke_url = "https://fpc8p1qglc.execute-api.us-east-1.amazonaws.com/dev/streams/"
    headers = {'Content-Type': 'application/json'}
    kin_topics = ['streaming-12baff1ff207-pin/record', 'streaming-12baff1ff207-geo/record', 'streaming-12baff1ff207-user/record']
    while True:
        sleep(random.randrange(0, 2))
        random_row = random.randint(0, 11000)
        engine = new_connector.create_db_connector()

        with engine.connect() as connection:

            pin_string = text(f"SELECT * FROM pinterest_data LIMIT {random_row}, 1")
            pin_selected_row = connection.execute(pin_string)
            
            for row in pin_selected_row:
                pin_result = dict(row._mapping)

            geo_string = text(f"SELECT * FROM geolocation_data LIMIT {random_row}, 1")
            geo_selected_row = connection.execute(geo_string)
            
            for row in geo_selected_row:
                geo_result = dict(row._mapping)

            user_string = text(f"SELECT * FROM user_data LIMIT {random_row}, 1")
            user_selected_row = connection.execute(user_string)
            
            for row in user_selected_row:
                user_result = dict(row._mapping)
            
            print(pin_result)
            print(geo_result)
            print(user_result)

            pin_payload = json.dumps({
            "StreamName": "streaming-12baff1ff207-pin",
            "Data": {
                    "value": pin_result
                    },
                    "PartitionKey": "partition-1"
                    }, default=str)
            
            geo_payload = json.dumps({
            "StreamName": "streaming-12baff1ff207-geo",
            "Data": {    
                    "value": geo_result
                    },
                    "PartitionKey": "partition-2"
                    }, default=str)
            
            user_payload = json.dumps({
            "StreamName": "streaming-12baff1ff207-user",
            "Data": {   
                    "value": user_result
                    },
                    "PartitionKey": "partition-3"
                    }, default=str)
            
            kin_pin_response = requests.request("PUT", kin_invoke_url + kin_topics[0], headers=headers, data=pin_payload, timeout=30)
            kin_geo_response = requests.request("PUT", kin_invoke_url + kin_topics[1], headers=headers, data=geo_payload, timeout=30)
            kin_user_response = requests.request("PUT", kin_invoke_url + kin_topics[2], headers=headers, data=user_payload, timeout=30)
            print(kin_pin_response.status_code)
            print(kin_geo_response.status_code)
            print(kin_user_response.status_code)






if __name__ == "__main__":
    run_infinite_post_data_loop()
    print('Working')
    
    


