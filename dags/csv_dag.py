from datetime import datetime, timedelta
from airflow import DAG
import pandas as pd
import csv
from pymongo import MongoClient
from airflow.operators.python_operator import PythonOperator
from datetime import datetime





default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 2, 27),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'schedule_interval' : None,
    'catchup' : False
}

dag4 = DAG('read_csv',default_args=default_args)

conn = MongoClient()

def read_csv(my_param,**kwargs):
    file = my_param
    df = pd.read_csv(f'data/{file}.csv')
    records = df.to_dict('records')
    conn.csv.data.insert_many(records)
    print("added")
    return df.to_string()

    # csv_file = "data/my.csv"
    # with open(csv_file, "r") as f:
            
    #             csv_reader = csv.DictReader(f)
    #             data = []
    #             for row in csv_reader:
    #                 data.append(row)
    
    # return data

dat= {'file': '{{ dag_run.conf["file"]  }}'}


read_csv_task = PythonOperator(task_id='read_csv_task',python_callable=read_csv,dag=dag4,op_kwargs={"my_param":dat['file']})

read_csv_task
