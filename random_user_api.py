import json
import requests
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator

#declaring the url for fetching data from API
base_url="https://randomuser.me/api/"

def extract_use_data(ti):
    """
    fetching the data using API and pushing into the xcom
    """
    response=requests.get(base_url)
    data=response.json()
    ti.xcom_push(key="extracted_data", value=data)

def write_json(ti):
    """
    fetching the extracted data from xcom and writing it into the JSON file
    """
    data=ti.xcom_pull(task_ids="extracted_data_task", key="extracted_data")
    timestamp=datetime.now().strftime("%Y-%m-%d-%H-%M")
    file_path=f"/home/varun/airflow/dags/output_{timestamp}.json"

    with open(file_path,'w')as json_file:
        json.dump(data, json_file,indent=4)

#Defining Dag
dag=DAG("random_user_api",start_date=datetime(2024,1,1),
        schedule_interval=timedelta(minutes=1), catchup=False)

extract_data_operator=PythonOperator(
    task_id="extracted_data_task",
    python_callable=extract_use_data,
    provide_context=True,
    dag=dag
)
write_data_operator=PythonOperator(
    task_id="write_data_task",
    python_callable=write_json,
    provide_context=True,
    dag=dag
)
#Setting the task dependencies
extract_data_operator >> write_data_operator
