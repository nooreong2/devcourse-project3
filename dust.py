from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago
from airflow.models import Variable

from datetime import datetime, timedelta
import requests
import logging


def get_Redshift_connection():
    # autocommit is False by default
    hook = PostgresHook(postgres_conn_id="redshift_dev_db")
    return hook.get_conn().cursor()


# 다운로드 및 데이터 정리 함수
def download_file(file_url, params):
    response = requests.get(file_url, params=params)  # 파일 URL에 GET 요청 보내기
    response.encoding = "euc-kr"  # 서버에서 전송된 인코딩을 지정
    decoded_content = response.text  # 응답의 내용을 디코딩하여 텍스트로 저장
    # 각 줄을 순회하며 #로 시작하지 않는 행들만 모으기
    cleaned_text = "\n".join([line for line in decoded_content.splitlines() if not line.startswith("#")])
    return cleaned_text


# ETL 함수
def etl():
    city = ["108", "119"]
    data = ""
    now = datetime.now()
    tm2 = execution_date.strftime("%Y%m%d%H%M")
    tm1 = (execution_date - timedelta(minutes=30)).strftime("%Y%m%d%H%M")
    # URL과 저장 경로 변수를 지정
    for stn in city:
        url = "https://apihub.kma.go.kr/api/typ01/url/kma_pm10.php"
        params = {"tm1": tm1, "tm2": tm2, "stn": stn, "authKey": Variable.get("weather_api_key")}

        response = download_file(url, params)
        data = data + response + "\n"

    print(data)


#     cur = get_Redshift_connection()
#     drop_recreate_sql = f"""DROP TABLE IF EXISTS {schema}.{table};
# CREATE TABLE {schema}.{table} (
#     date date,
#     stn int,
#     pm10 int,
#     flag char,
#     mqc char,
# );
# """
#     insert_sql = f"""INSERT INTO {schema}.{table} VALUES """ + ",".join(data)
#     logging.info(drop_recreate_sql)
#     logging.info(insert_sql)
#     try:
#         cur.execute(drop_recreate_sql)
#         cur.execute(insert_sql)
#         cur.execute("Commit;")
#     except Exception as e:
#         cur.execute("Rollback;")
#         raise


# DAG 정의
default_args = {
    "owner": "airflow",
    "retries": 1,
}

dag = DAG(
    "dust_to_redshift",
    default_args=default_args,
    start_date=datetime(2024, 6, 10),
    description="ETL DAG for KMA PM10 data",
    schedule_interval="*/30 * * * *",  # 30분마다 실행
)

# PythonOperator를 사용하여 ETL 작업 수행
run_etl = PythonOperator(
    task_id="run_etl",
    python_callable=etl,
    dag=dag,
)


run_etl()
