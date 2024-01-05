import requests
from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator

base_url="https://randomuser.me/api/"

def extract_use_data(ti,base_url):
    user_data=[]
    response=requests.get(base_url)