import json
import requests
import psycopg2
import pandas as pd
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator

#declaring the url for fetching data from API
base_url="https://randomuser.me/api/"

host="localhost"
user="postgres"
password="postgres"
dbname="userdb_airflow"
conn=psycopg2.connect(
    host=host, user=user, password=password, dbname=dbname
)
cursor=conn.cursor()
tables_info=[
        {'table_name': 'personal_details', 'xcom_key': 'personal_data_task', 'key':'person_data'},
        {'table_name': 'address', 'xcom_key': 'address_data_task', 'key':'address_data'},
        {'table_name': 'picture', 'xcom_key': 'picture_data_task', 'key':'picture_data'},
        {'table_name': 'login', 'xcom_key': 'login_data_task', 'key':'login_data'},
        {'table_name': 'location_tz', 'xcom_key': 'loc_tz_data_task', 'key':'loc_tz_data'},
        {'table_name': 'encrypted_detail', 'xcom_key': 'encrypt_data_task', 'key':'encrypt_data'},
        {'table_name': 'registration', 'xcom_key': 'registration_data_task', 'key':'reg_data'},
    ]

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
        "gender":personal_dict["gender"],
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
    encrypt_dict=user_data["results"][0]
    #dictionary is being created for the columns of encryption detail table. 
    #Value for each column is being extracted from the main extracted data(fetched from API)
    encrypt_data={
        "uuid":encrypt_dict["login"]["uuid"],
        "salt":encrypt_dict["login"]["salt"],
        "md5":encrypt_dict["login"]["md5"],
        "sha1":encrypt_dict["login"]["sha1"],
        "sha256":encrypt_dict["login"]["sha256"]
    }
    #The columns are being converted into DataFrame and then pushed to xcom.
    encrypt_df=pd.DataFrame([encrypt_data])
    ti.xcom_push(key="encrypt_data", value=encrypt_df)

def insert_into_db(ti,tables_info,**kwargs):
    """
    Here the DataFrames are fetched from
    """
    try:
        for current_table_info in tables_info:
            table_name=current_table_info['table_name']
            xcom_key=current_table_info['xcom_key']
            key=current_table_info['key']
            df=ti.xcom_pull(task_ids=xcom_key,key=key)
            columns=",".join(df.columns)
            value=",".join(["%s" for _ in df.columns])

            df = df.where(pd.notna(df), None)
            data_values = [tuple(row) for _, row in df.iterrows()]
            query = f"INSERT INTO {table_name} ({columns}) VALUES ({value})" #SQL query to insert into the columns
            try:
                cursor.executemany(query, data_values)
                print(f"Data inserted successfully into: {table_name}")
            except Exception as e:
                print(f"error inserting into {table_name} : {e}")
                conn.rollback()
                break
        else:
            conn.commit()
    except Exception as e:
        print(f"error inserting data {e}")
    finally:
        conn.close()

    

#Defining Dag
dag=DAG("random_user_api",start_date=datetime(2024,1,1),
        schedule_interval=timedelta(seconds=10), catchup=False)

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
    task_id="address_data_task",
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
    task_id="encrypt_data_task",
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
insert_into_db_operator=PythonOperator(
    task_id="insert_all_tables_task",
    python_callable=insert_into_db,
    provide_context=True,
    op_kwargs={'tables_info': tables_info},
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

personal_data_operator >> insert_into_db_operator
address_data_operator >> insert_into_db_operator
picture_data_operator >> insert_into_db_operator
login_data_operator >> insert_into_db_operator
loc_tz_operator >> insert_into_db_operator
encrypted_details_operaotor >> insert_into_db_operator
registration_operator >> insert_into_db_operator


