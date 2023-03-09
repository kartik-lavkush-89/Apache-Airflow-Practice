from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 2, 27),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'schedule_interval' : '0 12 * * *',
    'catchup' : False
}