from datetime import datetime, timedelta
from airflow import DAG
import pandas as pd
import csv
import sys
from pymongo import MongoClient
from airflow.operators.python_operator import PythonOperator
from datetime import datetime


sys.path.insert(0,"/home/uslsz0807/Documents/airflow")
from etl.first import gen_ran,load_to_mongo


        


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 2, 27),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'schedule_interval' : None,
    'catchup' : False
}

dag5 = DAG('etl',default_args=default_args)



generate_task = PythonOperator(task_id='generate_csv',python_callable=gen_ran,dag=dag5)
load_task = PythonOperator(task_id='load',python_callable=load_to_mongo,dag=dag5)


generate_task >> load_task
