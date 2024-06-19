from airflow import DAG
from airflow.operators.python import PythonOperator
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


def download_file(file_url, params):
    response = requests.get(file_url, params=params)
    response.encoding = "euc-kr"
    decoded_content = response.text
    cleaned_text = "\n".join([line for line in decoded_content.splitlines() if not line.startswith("#")])
    return cleaned_text


def etl(execution_date, schema, table):
    city = ["108", "119"]
    data = ""
    tm1 = (execution_date + timedelta(hours=9)).strftime("%Y%m%d%H%M")
    tm2 = (execution_date + timedelta(hours=9, minutes=55)).strftime("%Y%m%d%H%M")

    for stn in city:
        url = "https://apihub.kma.go.kr/api/typ01/url/kma_pm10.php"
        params = {"tm1": tm1, "tm2": tm2, "stn": stn, "authKey": Variable.get("weather_auth_key")}

        response = download_file(url, params)

        numeric_data = re.findall(r"\d{12},\s*\d+,\s*\d+", response)
        for index, line in enumerate(numeric_data):
            line = line.replace(" ", "")
            data += line
            if index < len(numeric_data) - 1 or stn != city[-1]:
                data += "\n"

    print("execution korea timedate: ", execution_date + timedelta(hours=9))
    print(data)

    cur = get_Redshift_connection()

    temp_table_name = f"{table}_temp"
    cur.execute(f"CREATE TABLE IF NOT EXISTS {schema}.{temp_table_name} (date TIMESTAMP, stn INT, pm10 INT)")

    cur.execute(
        f"SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = '{schema}' AND table_name = '{table}')"
    )
    table_exists = cur.fetchone()[0]

    if table_exists:
        cur.execute(f"INSERT INTO {schema}.{temp_table_name} SELECT * FROM {schema}.{table}")

    rows = data.strip().split("\n")

    for row in rows:
        row_data = row.split(",")
        date = datetime.strptime(row_data[0], "%Y%m%d%H%M")
        formatted_date = date.strftime("%Y-%m-%d %H:%M")
        stn = int(row_data[1])
        pm10 = int(row_data[2])
        cur.execute(f"INSERT INTO {schema}.{temp_table_name} (date, stn, pm10) VALUES (%s, %s, %s)", (formatted_date, stn, pm10))

    if table_exists:
        cur.execute(f"DROP TABLE IF EXISTS {schema}.{table}")

    cur.execute(f"ALTER TABLE {schema}.{temp_table_name} RENAME TO {table}")

    cur.connection.commit()
    cur.close()


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
run_etl = PythonOperator(
    task_id="run_etl",
    python_callable=etl,
    op_kwargs={"schema": "nooreong0503", "table": "dust"},
    dag=dag,
)
