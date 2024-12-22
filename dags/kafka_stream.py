from datetime import datetime 
from airflow import DAG
from airflow.operators.python import PythonOperator
from kafka import KafkaProducer
import time
import logging
import uuid
import json 
import requests


default_agrs = {
    'owner':"sushant",
    "start_date":datetime(2024,12,20,10,00)
}


def get_data():

    res = requests.get('https://randomuser.me/api/')
    res = res.json()
    res =  res['results'][0]
    print(res)
    return res


def data_formate(res):
    data = {}

    location = res['location']
    data['id'] = uuid.uuid4
    data['first_name'] = res['name']['first']
    data['last_name'] = res['name']['last']
    data['gender'] = res['gender']
    data['address'] = f"{str(location['street']['number'])} {location['street']['name']}, {location['city']}, {location['state']}, {location['country']}"
    data['post_code'] = location['postcode']
    data['email'] = res['email']
    data['username'] = res['login']['username']
    data['dob'] = res['dob']['date']
    data['registered_date'] = res['registered']['date']
    data['phone'] = res['phone']
    data['photo'] = res['picture']['medium']

    return data



def stream_data():
    try:
        
        producer = KafkaProducer(bootstrap_servers=['broker:29092'], max_block_ms=5000)
            
        curr_time = time.time() 

        while True:
            if time.time() > curr_time + 60:
                break

            try:
                res = get_data()
                res = data_formate(res)
                logging.info(res)
                print(res)
                producer.send("users_created",json.dumps(res).encode('utf-8'))
                producer.flush()


            except Exception as e:
                logging.error(f'An error occured{e}')
                continue

    except Exception as e:
        print(e)


with DAG("user_automation",
            default_args=default_agrs,
            schedule_interval='@daily',
            catchup=False) as dag:
    
    streaming_task = PythonOperator(
        task_id='stream_data_from_api',
        python_callable=stream_data
    )

