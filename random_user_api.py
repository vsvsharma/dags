import json
import requests
import pandas as pd
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
    raw_data=response.json()
    #fetched data will be pushed into the xcom, so that other functions can pull it.
    ti.xcom_push(key="extracted_data", value=raw_data)

     
def process_personal_details(ti):
    """
    Here, the data for personal detail table is being processed and 
    the whole data fetched from api is being pulled here.
    """
    user_data=ti.xcom_pull(task_ids="extracted_data_task", key="extracted_data")
    personal_dict=user_data["results"][0]
    #dictionary is being created for the columns of personal detail table. 
    #Value for each column is being extracted from the main extracted data(fetched from API)
    person_data={
        "uuid":personal_dict["login"]["uuid"],
        "title":personal_dict["name"]["title"],
        "first_name":personal_dict["name"]["first"],
        "last_name":personal_dict["name"]["last"],
        "email":personal_dict["email"],
        "date_of_birth":personal_dict["dob"]["date"],
        "age":personal_dict["dob"]["age"],
        "phone":personal_dict["phone"],
        "cell":personal_dict["cell"],
        "nationality":personal_dict["nat"]
    }
    #The columns are being converted into DataFrame and then pushed to xcom.
    person_df=pd.DataFrame([person_data])
    ti.xcom_push(key="person_data", value=person_df)

def process_address(ti):
    """
    Here, the data for address table is being processed and 
    the whole data fetched from api is being pulled here.
    """
    user_data=ti.xcom_pull(task_ids="extracted_data_task", key="extracted_data")
    address_dict=user_data["results"][0]
    #dictionary is being created for the columns of address table. 
    #Value for each column is being extracted from the main extracted data(fetched from API).
    address_data={
        "uuid":address_dict["login"]["uuid"],
        "street_number":address_dict["location"]["street"]["number"],
        "street_name":address_dict["location"]["street"]["name"],
        "city":address_dict["location"]["city"],
        "state":address_dict["location"]["state"],
        "country":address_dict["location"]["country"],
        "postcode":address_dict["location"]["postcode"]
    }
    #The columns are being converted into DataFrame and then pushed to xcom.
    address_df=pd.DataFrame([address_data])
    ti.xcom_push(key="address_data", value=address_df)

def process_picture(ti):
    """
    Here, the data for picture table is being processed and 
    the whole data fetched from api is being pulled here.
    """
    user_data=ti.xcom_pull(task_ids="extracted_data_task", key="extracted_data")
    picture_dict=user_data["results"][0]
    #dictionary is being created for the columns of picture table. 
    #Value for each column is being extracted from the main extracted data(fetched from API)
    picture_data={
        "uuid":picture_dict["login"]["uuid"],
        "profile_pic_large":picture_dict["picture"]["large"],
        "profile_pic_medium":picture_dict["picture"]["medium"],
        "profile_pic_thumbnail":picture_dict["picture"]["thumbnail"]
    }
    #The columns are being converted into DataFrame and then pushed to xcom.
    picture_df=pd.DataFrame([picture_data])
    ti.xcom_push(key="picture_data", value=picture_df)

def process_login(ti):
    """
    Here, the data for login table is being processed and 
    the whole data fetched from api is being pulled here.
    """
    user_data=ti.xcom_pull(task_ids="extracted_data_task", key="extracted_data")
    login_dict=user_data["results"][0]
    #dictionary is being created for the columns of login table. 
    #Value for each column is being extracted from the main extracted data(fetched from API).
    login_data={
        "uuid":login_dict["login"]["uuid"],
        "username":login_dict["login"]["username"],
        "password":login_dict["login"]["password"]
    }
    #The columns are being converted into DataFrame and then pushed to xcom.
    login_df=pd.DataFrame([login_data])
    ti.xcom_push(key="login_data", value=login_df)

def process_location_tz(ti):
    """
    Here, the data for location table is being processed and 
    the whole data fetched from api is being pulled here.
    """
    user_data=ti.xcom_pull(task_ids="extracted_data_task", key="extracted_data")
    loc_tz_dict=user_data["results"][0]
    #dictionary is being created for the columns of location table. 
    #Value for each column is being extracted from the main extracted data(fetched from API).
    loc_tz_data={
        "uuid":loc_tz_dict["login"]["uuid"],
        "latitude":loc_tz_dict["location"]["coordinates"]["latitude"],
        "longitude":loc_tz_dict["location"]["coordinates"]["longitude"],
        "timezone_offset":loc_tz_dict["location"]["timezone"]["offset"],
        "timezone_description":loc_tz_dict["location"]["timezone"]["description"]
    }
    #The columns are being converted into DataFrame and then pushed to xcom.
    loc_tz_df=pd.DataFrame([loc_tz_data])
    ti.xcom_push(key="loc_tz_data", value=loc_tz_df)

def process_registration(ti):
    """
    Here, the data for registration table is being processed and 
    the whole data fetched from api is being pulled here.
    """
    user_data=ti.xcom_pull(task_ids="extracted_data_task", key="extracted_data")
    reg_dict=user_data["results"][0]
    #dictionary is being created for the columns of registration table. 
    #Value for each column is being extracted from the main extracted data(fetched from API).
    reg_data={
        "uuid":reg_dict["login"]["uuid"],
        "registration_date":reg_dict["registered"]["date"],
        "registration_age":reg_dict["registered"]["age"]
    }
    #The columns are being converted into DataFrame and then pushed to xcom.
    reg_df=pd.DataFrame([reg_data])
    ti.xcom_push(key="reg_data", value=reg_df)

def process_encrypted_details(ti):
    """
    Here, the data for encrypted detail table is being processed and 
    the whole data fetched from api is being pulled here.
    """
    user_data=ti.xcom_pull(task_ids="extracted_data_task", key="extracted_data")
    encryp_dict=user_data["results"][0]
    #dictionary is being created for the columns of encryption detail table. 
    #Value for each column is being extracted from the main extracted data(fetched from API)
    encryp_data={
        "uuid":encryp_dict["login"]["uuid"],
        "salt":encryp_dict["login"]["salt"],
        "md5":encryp_dict["login"]["md5"],
        "sha1":encryp_dict["login"]["sha1"],
        "sha256":encryp_dict["login"]["sha256"]
    }
    #The columns are being converted into DataFrame and then pushed to xcom.
    encryp_df=pd.DataFrame([encryp_data])
    ti.xcom_push(key="encryp_data", value=encryp_df)

#Defining Dag
dag=DAG("random_user_api",start_date=datetime(2024,1,1),
        schedule_interval=timedelta(minutes=1), catchup=False)

#Operators are defined
"""
Operators are defined to call the different functions and task Ids are defined respectively.
"""
extract_data_operator=PythonOperator(
    task_id="extracted_data_task",
    python_callable=extract_use_data,
    provide_context=True,
    dag=dag
)
personal_data_operator=PythonOperator(
    task_id="personal_data_task",
    python_callable=process_personal_details,
    provide_context=True,
    dag=dag
)
address_data_operator=PythonOperator(
    task_id="adderess_data_task",
    python_callable=process_address,
    provide_context=True,
    dag=dag
)
picture_data_operator=PythonOperator(
    task_id="picture_data_task",
    python_callable=process_picture,
    provide_context=True,
    dag=dag
)
login_data_operator=PythonOperator(
    task_id="login_data_task",
    python_callable=process_login,
    provide_context=True,
    dag=dag
)
loc_tz_operator=PythonOperator(
    task_id="loc_tz_data_task",
    python_callable=process_location_tz,
    provide_context=True,
    dag=dag
)
encrypted_details_operaotor=PythonOperator(
    task_id="encryp_data_task",
    python_callable=process_encrypted_details,
    provide_context=True,
    dag=dag
)
registration_operator=PythonOperator(
    task_id="registration_data_task",
    python_callable=process_registration,
    provide_context=True,
    dag=dag
)

#Set the task dependencies
"""
here task dependencies are defined to make that clear which operator will execute first 
and then other dependent operator respectively each after another.
"""
extract_data_operator >> personal_data_operator
extract_data_operator >> address_data_operator
extract_data_operator >> picture_data_operator
extract_data_operator >> login_data_operator
extract_data_operator >> loc_tz_operator
extract_data_operator >> encrypted_details_operaotor
extract_data_operator >> registration_operator


