from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'wulfried_TAYO',
    'depends_on_past': False,
    'start_date': datetime.now(),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    #'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
    # 'wait_for_downstream': False,
    # 'dag': dag,
    # 'sla': timedelta(hours=2),
    # 'execution_timeout': timedelta(seconds=300),
    # 'on_failure_callback': some_function,
    # 'on_success_callback': some_other_function,
    # 'on_retry_callback': another_function,
    # 'sla_miss_callback': yet_another_function,
    # 'trigger_rule': 'all_success'
}

def get_data():
    import json
    import requests

    res = requests.get('https://randomuser.me/api/')
    res = res.json()['results'][0]

    return res

def format_data(res):
    data = {}
    location = res['location']
    data['first_name'] = res['name']['first']
    data['last_name'] = res['name']['last']
    data['gender'] = res['gender']
    data['address'] = f"{str(location['street']['number'])} {location['street']['name']} " \
                      f"{location['city']}, {location['state']}, {location['country']}"
    data['postcode'] = str(location['postcode'])
    data['email'] = res['email']
    data['username'] = res['login']['username']
    data['dob'] = res['dob']
    data['registered_date'] = res['registered']['date']
    data['phone'] = res['phone']
    data['picture'] = res['picture']['medium']

    return data

def stream_data():
    import json
    from kafka import KafkaProducer
    import time
    import logging

    producer = KafkaProducer(bootstrap_servers=['broker:29092'], max_block_ms=5000)
    current_time = time.time()

    while True:
        if time.time() > current_time + 60:
            break
        try:
            response = get_data()
            response = format_data(response)
            producer.send('users_create', json.dumps(response).encode('utf-8'))
        except Exception as e:
            logging.error(f" we have an error occured : {e}")
            continue


with DAG(
    'user_automation',
    default_args=default_args,
    description='kafka DAG',
    schedule_interval='@daily',
    catchup=False) as dag:

    streaming_task = PythonOperator(
        task_id='stream_data_from_API',
        python_callable=stream_data
    )

#stream_data()