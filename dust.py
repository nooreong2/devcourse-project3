from airflow import DAG
from airflow.operators.python import PythonOperator  # Airflow 2.x에서는 이렇게 import
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago
from airflow.models import Variable
from airflow.exceptions import AirflowException

from datetime import datetime, timedelta
import requests
import logging
import re
import uuid


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
    tm2 = (execution_date + timedelta(hours=9, minutes=55)).strftime("%Y%m%d%H%M")

    for stn in city:
        url = "https://apihub.kma.go.kr/api/typ01/url/kma_pm10.php"
        params = {"tm1": tm1, "tm2": tm2, "stn": stn, "authKey": Variable.get("weather_auth_key")}

        response = download_file(url, params)
        # 헤더를 제외하고 숫자 데이터만 가져오기
        numeric_data = re.findall(r"\d{12},\s*\d+,\s*\d+", response)
        for index, line in enumerate(numeric_data):
            line = line.replace(" ", "")  # 공백 제거
            data += line
            if index < len(numeric_data) - 1 or stn != city[-1]:  # 마지막 행이 아니거나 마지막 도시가 아니면 개행 추가
                data += "\n"

    print("execution korea timedate: ", execution_date + timedelta(hours=9))
    print(data)

    cur = get_Redshift_connection()
    select_sql = f"""SELECT * FROM {schema}.{table}"""

    sql = f"""DROP TABLE IF EXISTS {schema}.temp_{table};CREATE TABLE {schema}.temp_{table} AS """
    sql += select_sql
    cur.execute(sql)

    cur.execute(f"""SELECT COUNT(1) FROM {schema}.temp_{table}""")
    count = cur.fetchone()[0]
    if count == 0:
        raise ValueError(f"{schema}.{table} didn't have any record")

    try:
        sql = f"""DROP TABLE IF EXISTS {schema}.{table};ALTER TABLE {schema}.temp_{table} RENAME to {table};"""
        sql += "COMMIT;"
        logging.info(sql)
        cur.execute(sql)
    except Exception as e:
        cur.execute("ROLLBACK")
        logging.error("Failed to sql. Completed ROLLBACK!")
        raise AirflowException("")


# DAG 정의
default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "dust_to_redshift",
    default_args=default_args,
    start_date=datetime(2024, 6, 12, 20),
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
