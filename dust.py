from airflow import DAG
from airflow.operators.python import PythonOperator  # Airflow 2.x에서는 이렇게 import
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago
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
def etl(execution_date, **kwargs):  # execution_date 인수 추가
    city = ["108", "119"]
    data = ""
    tm2 = execution_date.strftime("%Y%m%d%H%M")
    tm1 = (execution_date - timedelta(minutes=30)).strftime("%Y%m%d%H%M")

    for stn in city:
        url = "https://apihub.kma.go.kr/api/typ01/url/kma_pm10.php"
        params = {"tm1": tm1, "tm2": tm2, "stn": stn, "authKey": "Rd1YWyRfRbmdWFskX-W5ag"}

        response = download_file(url, params)
        data += response + "\n"

    print("execution date: ", execution_date)
    print(data)

    # 아래 코드는 필요에 따라 다시 활성화
    # cur = get_Redshift_connection()
    # drop_recreate_sql = f"""DROP TABLE IF EXISTS {schema}.{table};
    # CREATE TABLE {schema}.{table} (
    #     date date,
    #     stn int,
    #     pm10 int,
    #     flag char,
    #     mqc char,
    # );
    # """
    # insert_sql = f"""INSERT INTO {schema}.{table} VALUES """ + ",".join(data)
    # logging.info(drop_recreate_sql)
    # logging.info(insert_sql)
    # try:
    #     cur.execute(drop_recreate_sql)
    #     cur.execute(insert_sql)
    #     cur.execute("Commit;")
    # except Exception as e:
    #     cur.execute("Rollback;")
    #     raise


# DAG 정의
default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "dust_to_redshift",
    default_args=default_args,
    start_date=datetime(2024, 6, 10),
    description="ETL DAG for KMA PM10 data",
    schedule_interval="*/0 1 * * *",
)

# PythonOperator를 사용하여 ETL 작업 수행
run_etl = PythonOperator(
    task_id="run_etl",
    python_callable=etl,
    dag=dag,
)

# 태스크를 직접 호출하지 않습니다.
# run_etl()
