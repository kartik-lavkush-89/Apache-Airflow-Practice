# from dags.first_dag import default_args
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from pymongo import MongoClient
# from src.schemas.user import userEntity

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 2, 27),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'schedule_interval' : '0 12 * * *',
    'catchup' : False
}


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

def read_data(my_params,**kwargs):
    name = my_params
    data = conn.mydatabase.mycollection.find_one({"name": name})
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
# dat1={'name': '{{ dag_run.conf["name"]  }}'}
insert_task = PythonOperator(task_id='insert_data', python_callable=insert_data, dag=dag2,op_kwargs={"my_param":{**dat}})
# read_task = PythonOperator(task_id='read_data', python_callable=read_data, dag=dag2,op_kwargs={"my_param":{**dat1.name}})
# update_task = PythonOperator(task_id='update_data', python_callable=update_data, dag=dag2)
# delete_task = PythonOperator(task_id='delete_data', python_callable=delete_data, dag=dag2)

insert_task 
