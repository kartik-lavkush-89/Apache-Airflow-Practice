from datetime import datetime, timedelta
from airflow import DAG
import random
from twilio.rest import Client
from airflow.operators.python_operator import PythonOperator

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 2, 27),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'schedule_interval' : '0 12 * * *',
    'catchup' : False
}



'''DAG 3'''


dag3 = DAG('sending_OTP', description = 'OTP', default_args=default_args)

otp = random.randrange(000000,999999)


def send_otp():
    # account_sid = os.environ.get('ACCOUNT_SID')
    # auth_token = os.environ.get('AUTH_TOKEN')
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