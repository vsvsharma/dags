from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import requests
import json
from kafka import KafkaProducer

def fetch_data_from_api(ti):
    kafka_bootstrap_servers = "localhost:9092"
    kafka_topic = "random_user_fetching"
    api_url = "https://randomuser.me/api/"

    producer = KafkaProducer(bootstrap_servers=kafka_bootstrap_servers)

    response = requests.get(api_url)
    response.raise_for_status()
    data = response.json()

    serialized_data = json.dumps(data).encode('utf-8')
    producer.send(kafka_topic, value=serialized_data)
    producer.flush()

dag = DAG(
    'kafka_producer_dag',
    description='DAG to fetch random user data and send to Kafka',
    start_date=datetime(2023, 1, 1),
    schedule_interval=timedelta(seconds=5),
)

fetch_and_send_task = PythonOperator(
    task_id='fetch_and_send_to_kafka',
    python_callable=fetch_data_from_api,
    provide_context=True,
    dag=dag,
)

fetch_and_send_task