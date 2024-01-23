from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import json
from kafka import KafkaConsumer

def kafka_consumer(ti):
    consumer = KafkaConsumer(
        'random_user_fetching',
        bootstrap_servers='localhost:9092',
        max_poll_records=20,
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        auto_offset_reset='earliest',
        enable_auto_commit=False,  
        group_id='my_grp'
    )
    data_list = []
    max_messages = 20

    try:
        for message in consumer:
            data = message.value
            data_list.append(data)
            if len(data_list) >= max_messages:
                break
    except Exception as e:
        print(f"Error in Kafka consumer: {e}")
    finally:
        consumer.commit()

    return(data_list)

dag = DAG(
    'kafka_consumer_dag',
    description='DAG to consume random user data from topic',
    start_date=datetime(2023, 1, 1),
    schedule_interval=timedelta(minutes=5),
)

fetch_from_topic = PythonOperator(
    task_id='fetch_and_send_to_kafka',
    python_callable=kafka_consumer,
    provide_context=True,
    dag=dag,
)

fetch_from_topic

    