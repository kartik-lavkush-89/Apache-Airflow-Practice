from fastapi import FastAPI
import requests
import random
import json
from src.models.user import Detail, OTP, File

app = FastAPI()


@app.post("/trigger_mongo_dag")
async def trigger_dag(info : Detail):
    dag_id = info.dag
    dag_run_id = random.randint(000000, 999999)
    url = "http://localhost:8080/api/v1/dags/{}/dagRuns".format(dag_id)
    payload = json.dumps({
    "conf": {"name":info.name,"age":info.age},
    "dag_run_id": f"{dag_run_id}"
    })
    headers = {
    'Authorization': 'Basic YWRtaW46YWRtaW4=',
    'Content-Type': 'application/json'
    }

    response = requests.request("POST", url, headers=headers, data=payload)
    if(response.status_code==200):
        return {"message": "DAG triggered"}
    return json.loads(response.text)

@app.post("/trigger_otp_dag")
async def trigger_dag(info : OTP):
    dag_id = info.dag
    dag_run_id = random.randint(000000, 999999)
    url = "http://localhost:8080/api/v1/dags/{}/dagRuns".format(dag_id)
    payload = json.dumps({
    "conf": {"phone":info.phone},
    "dag_run_id": f"{dag_run_id}"
    })
    headers = {
    'Authorization': 'Basic YWRtaW46YWRtaW4=',
    'Content-Type': 'application/json'
    }

    response = requests.request("POST", url, headers=headers, data=payload)
    if(response.status_code==200):
        return {"message": "DAG triggered"}
    return json.loads(response.text)

@app.post("/trigger_csv_dag")
async def trigger_dag(info : File):
    dag_id = info.dag
    dag_run_id = random.randint(000000, 999999)
    url = "http://localhost:8080/api/v1/dags/{}/dagRuns".format(dag_id)
    payload = json.dumps({
    "conf": {"file":info.file},
    "dag_run_id": f"{dag_run_id}"
    })
    headers = {
    'Authorization': 'Basic YWRtaW46YWRtaW4=',
    'Content-Type': 'application/json'
    }

    response = requests.request("POST", url, headers=headers, data=payload)
    if(response.status_code==200):
        return {"message": "DAG triggered"}
    return json.loads(response.text)

if __name__ == '__main__':
    app.run()