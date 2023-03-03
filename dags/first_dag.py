from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator



default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 2, 27),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'schedule_interval' : '0 12 * * *',
    'catchup' : False
}


'''DAG 1'''

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
 




