from fastapi import FastAPI
from fastapi import APIRouter, HTTPException, Response, Header
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from twilio.rest import Client
import random
import requests
from pymongo import MongoClient
from bson import objectid
from pydantic import BaseModel


app = FastAPI()

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 2, 27),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'schedule_interval' : '0 12 * * *',
    'catchup' : False
}




dag1 = DAG('hello_world', description='Hello World DAG', default_args=default_args)

def print_hello():
    print("hello ------------------------")
    return 'Hello world from first Airflow DAG!'

def print_hi():
    print("hi ------------------------")
    return 'Hi i am here'


hello_operator = PythonOperator(task_id='hello_task', python_callable=print_hello, dag=dag1)
hi_operator = PythonOperator(task_id='hi_task', python_callable=print_hi, dag=dag1)
hi_operator1 = PythonOperator(task_id='hi_task1', python_callable=print_hi, dag=dag1)

hello_operator >> [hi_operator,hi_operator1]




# '''Mongo'''




dag2 = DAG('mongo_crud', description = 'MongoDB', default_args=default_args,
           )

def userEntity(item) -> dict:
    return {
        "id" : str(item["_id"]),
        "name" : item["name"],
        "age" : item["age"]
    }

conn = MongoClient()

def insert_data(my_param,**kwargs):
    
    data = my_param
    conn.mydatabase.mycollection.insert_one(data)
    print(data)
    return {"message" : "data_added_successfully"}

def read_data():
    data = conn.mydatabase.mycollection.find_one({"name": "John"})
    print("data found")
    return userEntity(data)

def update_data():
    conn.mydatabase.mycollection.find_one_and_update({"name": "John"}, {"$set": {"age": 35}})
    data = conn.mydatabase.mycollection.find_one({"name": "John"})
    print(data)
    return {"message" : "data_updated_successfully"}

def delete_data():
    data = conn.mydatabase.mycollection.find_one_and_delete({"name" : "John"})
    print(data)
    return {"message" : "data_deleted_successfully"}


dat= {'name': '{{ dag_run.conf["name"]  }}','age': '{{ dag_run.conf["age"] }}'}
insert_task = PythonOperator(task_id='insert_data', python_callable=insert_data, dag=dag2,op_kwargs={"my_param":{**dat}})
# read_task = PythonOperator(task_id='read_data', python_callable=read_data, dag=dag2)
# update_task = PythonOperator(task_id='update_data', python_callable=update_data, dag=dag2)
# delete_task = PythonOperator(task_id='delete_data', python_callable=delete_data, dag=dag2)

insert_task #>> read_task >> update_task 





dag3 = DAG('sending_OTP', description = 'OTP', default_args=default_args)

otp = random.randrange(000000,999999)

@app.post('/otp')
def send_otp():
    account_sid = "ACba3ab41cd32568b368e387dca973c30d"
    auth_token = "ca36ffdc79bf68868d094cded7419ff1"
    client = Client(account_sid, auth_token)
    message = client.messages.create(
                    body="Hello! Your otp for registration is - " + str(otp),
                    from_="+16086022741",
                    to ='+91' + str('8923933990')
                )
    print(otp)
    return {"message" : f"OTP_sent {otp}"}

otp_sent = PythonOperator(task_id='send_otp', python_callable=send_otp, dag=dag3)

otp_sent



# dag4 = DAG('mongo_crud_api', description='MongoDB', default_args=default_args)

# def userEntity(item) -> dict:
#     return {
#         "id" : str(item["_id"]),
#         "name" : item["name"],
#         "age" : item["age"]
#     }

# def insert_data():
#     data = {'name': 'John', 'age': 30}
#     resp = requests.post('http://localhost:8080/api/v1/dags/mongo_crud/dag_runs', json=data)
#     print(resp.json())
#     return {"message" : "data_added_successfully"}

# def read_data():
#     resp = requests.get('http://localhost:8080/api/v1/dags/mongo_crud/dag_runs')
#     data = resp.json()[0]['output']
#     print("data found")
#     return userEntity(data)

# def update_data():
#     data = {"age": 35}
#     resp = requests.patch('http://localhost:8080/api/v1/dags/mongo_crud/dag_runs/1', json=data)
#     print(resp.json())
#     return {"message" : "data_updated_successfully"}

# def delete_data():
#     resp = requests.delete('http://localhost:8080/api/v1/dags/mongo_crud/dag_runs/1')
#     print(resp.json())
#     return {"message" : "data_deleted_successfully"}

# insert_task = PythonOperator(task_id='insert_data', python_callable=insert_data, dag=dag4)
# read_task = PythonOperator(task_id='read_data', python_callable=read_data, dag=dag4)
# update_task = PythonOperator(task_id='update_data', python_callable=update_data, dag=dag4)
# delete_task = PythonOperator(task_id='delete_data', python_callable=delete_data, dag=dag4)

# insert_task >> read_task >> update_task >> delete_task
