from airflow import DAG
from airflow.operators.python import PythonOperator  # Airflow 2.x에서는 이렇게 import
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago
from airflow.models import Variable
from airflow.exceptions import AirflowException

from datetime import datetime, timedelta
import requests
import logging


def get_Redshift_connection():
    hook = PostgresHook(postgres_conn_id="redshift_dev_db")
    return hook.get_conn().cursor()


# 다운로드 및 데이터 정리 함수
def download_file(file_url, params):
    response = requests.get(file_url, params=params)
    response.encoding = "euc-kr"
    decoded_content = response.text
    cleaned_text = "\n".join([line for line in decoded_content.splitlines() if not line.startswith("#")])
    return cleaned_text


# ETL 함수
def etl(execution_date, schema, table):
    city = ["108", "119"]
    data = ""
    tm1 = (execution_date + timedelta(hours=9)).strftime("%Y%m%d%H%M")
    tm2 = (execution_date + timedelta(hours=10)).strftime("%Y%m%d%H%M")

    for stn in city:
        url = "https://apihub.kma.go.kr/api/typ01/url/kma_pm10.php"
        params = {"tm1": tm1, "tm2": tm2, "stn": stn, "authKey": Variable.get("weather_auth_key")}

        response = download_file(url, params)
        data += response + "\n"
    print(data)
    print("execution korea timedate: ", execution_date + timedelta(hours=9))

    # cur = get_Redshift_connection()

    # cur.execute(f"CREATE TABLE IF NOT EXISTS {schema}.{table} (date DATE, stn INT, pm10 INT, flag CHAR, mqc CHAR)")


# DAG 정의
default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "dust_to_redshift",
    default_args=default_args,
    start_date=datetime(2024, 6, 11),
    description="ETL DAG for KMA PM10 data",
    schedule_interval="0 * * * *",
)

# PythonOperator를 사용하여 ETL 작업 수행
run_etl = PythonOperator(
    task_id="run_etl",
    python_callable=etl,
    op_kwargs={"schema": "nooreong0503", "table": "dust"},
    dag=dag,
)
