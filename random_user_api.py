import json
import psycopg2
import pandas as pd
from airflow import DAG
from kafka import KafkaConsumer
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator

#declaring the url for fetching data from API


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

    ti.xcom_push(key="extracted_data", value=data_list)


     
def process_personal_details(ti):
    """
    Here, the data for personal detail table is being processed and 
    the whole data fetched from api is being pulled here.
    """
    user_data=ti.xcom_pull(task_ids="extracted_data_task", key="extracted_data")
    person_data=[]
    for item in user_data:
        results=item['results']
    #dictionary is being created for the columns of personal detail table. 
    #Value for each column is being extracted from the main extracted data(fetched from API)
    for result in results:
        uuid=result["login"]["uuid"]
        gender=result["gender"]
        title=result["name"]["title"]
        first_name=result["name"]["first"]
        last_name=result["name"]["last"]
        email=result["email"]
        date_of_birth=result["dob"]["date"]
        age=result["dob"]["age"]
        phone=result["phone"]
        cell=result["cell"]
        nationality=result["nat"]
        
        personal_dict={
            "uuid":uuid,
            "gender":gender,
            "title":title,
            "first_name":first_name,
            "last_name":last_name,
            "email":email,
            "date_of_birth":date_of_birth,
            "age":age,
            "phone":phone,
            "cell":cell,
            "nationality":nationality
        }
        person_data.append(personal_dict)
    
    #The columns are being converted into DataFrame and then pushed to xcom.
    person_df=pd.DataFrame(person_data)
    print(f"The length of person df is: {len(person_df)}")
    ti.xcom_push(key="person_data", value=person_df)

def process_address(ti):
    """
    Here, the data for address table is being processed and 
    the whole data fetched from api is being pulled here.
    """
    user_data=ti.xcom_pull(task_ids="extracted_data_task", key="extracted_data")
    address_data=[]
    for item in user_data:
        results=item['results']
    
    #dictionary is being created for the columns of address table. 
    #Value for each column is being extracted from the main extracted data(fetched from API).
    for result in results:
        uuid=result["login"]["uuid"]
        street_number=result["location"]["street"]["number"]
        street_name=result["location"]["street"]["name"]
        city=result["location"]["city"]
        state=result["location"]["state"]
        country=result["location"]["country"]
        postcode=result["location"]["postcode"]

        address_dict={
            "uuid":uuid,
            "street_number":street_number,
            "street_name":street_name,
            "city":city,
            "state":state,
            "country":country,
            "postcode":postcode
        }
        address_data.append(address_dict)
    
    #The columns are being converted into DataFrame and then pushed to xcom.
    address_df=pd.DataFrame(address_data)
    print(f"The length of address df is: {len(address_df)}")
    ti.xcom_push(key="address_data", value=address_df)

def process_picture(ti):
    """
    Here, the data for picture table is being processed and 
    the whole data fetched from api is being pulled here.
    """
    user_data=ti.xcom_pull(task_ids="extracted_data_task", key="extracted_data")
    picture_data=[]
    for item in user_data:
        results=item['results']
    #dictionary is being created for the columns of picture table. 
    #Value for each column is being extracted from the main extracted data(fetched from API)
    for result in results:
        uuid=result["login"]["uuid"],
        profile_pic_large=result["picture"]["large"],
        profile_pic_medium=result["picture"]["medium"],
        profile_pic_thumbnail=result["picture"]["thumbnail"]

        picture_dict={
            "uuid":uuid,
            "profile_pic_large":profile_pic_large,
            "profile_pic_medium":profile_pic_medium,
            "profile_pic_thumbnail":profile_pic_thumbnail
        }
        picture_data.append(picture_dict)
    
    #The columns are being converted into DataFrame and then pushed to xcom.
    picture_df=pd.DataFrame(picture_data)
    print(f"The length of picture df is: {len(picture_df)}")
    ti.xcom_push(key="picture_data", value=picture_df)

def process_login(ti):
    """
    Here, the data for login table is being processed and 
    the whole data fetched from api is being pulled here.
    """
    user_data=ti.xcom_pull(task_ids="extracted_data_task", key="extracted_data")
    login_data=[]
    for item in user_data:
        results=item['results']
    #dictionary is being created for the columns of login table. 
    #Value for each column is being extracted from the main extracted data(fetched from API).
    for result in results:
        uuid=result["login"]["uuid"]
        username=result["login"]["username"]
        password=result["login"]["password"]

        login_dict={
            "uuid":uuid,
            "username":username,
            "password":password
        }
        login_data.append(login_dict)
    #The columns are being converted into DataFrame and then pushed to xcom.
    login_df=pd.DataFrame(login_data)
    print(f"The length of login df is: {len(login_df)}")
    ti.xcom_push(key="login_data", value=login_df)

def process_location_tz(ti):
    """
    Here, the data for location table is being processed and 
    the whole data fetched from api is being pulled here.
    """
    user_data=ti.xcom_pull(task_ids="extracted_data_task", key="extracted_data")
    loc_tz_data=[]
    for item in user_data:
        results=item['results']
    #dictionary is being created for the columns of location table. 
    #Value for each column is being extracted from the main extracted data(fetched from API).
    for result in results:
        uuid=result["login"]["uuid"]
        latitude=result["location"]["coordinates"]["latitude"]
        longitude=result["location"]["coordinates"]["longitude"]
        timezone_offset=result["location"]["timezone"]["offset"]
        timezone_description=result["location"]["timezone"]["description"]

        loc_tz_dict={
            "uuid":uuid,
            "latitude":latitude,
            "longitude":longitude,
            "timezone_offset":timezone_offset,
            "timezone_description":timezone_description
        }
        loc_tz_data.append(loc_tz_dict)
    #The columns are being converted into DataFrame and then pushed to xcom.
    loc_tz_df=pd.DataFrame(loc_tz_data)
    print(f"The length of loc_tz df is: {len(loc_tz_df)}")
    ti.xcom_push(key="loc_tz_data", value=loc_tz_df)

def process_registration(ti):
    """
    Here, the data for registration table is being processed and 
    the whole data fetched from api is being pulled here.
    """
    user_data=ti.xcom_pull(task_ids="extracted_data_task", key="extracted_data")
    reg_data=[]
    for item in user_data:
        results=item['results']
    #dictionary is being created for the columns of registration table. 
    #Value for each column is being extracted from the main extracted data(fetched from API).
    for result in results:
        uuid=result["login"]["uuid"]
        registration_date=result["registered"]["date"]
        registration_age=result["registered"]["age"]

        reg_dict={
            "uuid":uuid,
            "registration_date":registration_date,
            "registration_age":registration_age
        }
        reg_data.append(reg_dict)
    #The columns are being converted into DataFrame and then pushed to xcom.
    reg_df=pd.DataFrame(reg_data)
    print(f"The length of reg df is: {len(reg_df)}")
    ti.xcom_push(key="reg_data", value=reg_df)

def process_encrypted_details(ti):
    """
    Here, the data for encrypted detail table is being processed and 
    the whole data fetched from api is being pulled here.
    """
    user_data=ti.xcom_pull(task_ids="extracted_data_task", key="extracted_data")
    encrypt_data=[]
    for item in user_data:
        results=item['results']
    #dictionary is being created for the columns of encryption detail table. 
    #Value for each column is being extracted from the main extracted data(fetched from API)
    for result in results:
        uuid=result["login"]["uuid"]
        salt=result["login"]["salt"]
        md5=result["login"]["md5"]
        sha1=result["login"]["sha1"]
        sha256=result["login"]["sha256"]

        encrypt_dict={
            "uuid":uuid,
            "salt":salt,
            "md5":md5,
            "sha1":sha1,
            "sha256":sha256
        }
        encrypt_data.append(encrypt_dict)
    #The columns are being converted into DataFrame and then pushed to xcom.
    encrypt_df=pd.DataFrame(encrypt_data)
    print(f"The length of encrypt df is: {len(encrypt_df)}")
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
        schedule_interval=timedelta(minutes=2), catchup=False)

#Operators are defined
"""
Operators are defined to call the different functions and task Ids are defined respectively.
"""
extract_data_operator=PythonOperator(
    task_id="extracted_data_task",
    python_callable=kafka_consumer,
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


