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

    # 테이블 작업을 위한 트랜잭션 블록 시작
    cur.execute("BEGIN")

    # 임시 테이블 생성
    temp_table_name = f"{table}_temp"
    cur.execute(f"CREATE TABLE IF NOT EXISTS {schema}.{temp_table_name} (date TIMESTAMP, stn INT, pm10 INT)")

    # 기존 테이블이 존재하는지 확인
    cur.execute(
        f"SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = '{schema}' AND table_name = '{table}')"
    )
    table_exists = cur.fetchone()[0]

    if table_exists:
        # 기존 데이터를 temp 테이블에 복사
        cur.execute(f"INSERT INTO {schema}.{temp_table_name} SELECT * FROM {schema}.{table}")

    # 새로운 데이터를 temp 테이블에 삽입
    rows = data.strip().split("\n")
    for row in rows:
        # 각 줄에서 데이터를 추출합니다.
        row_data = row.split(",")
        date = datetime.strptime(row_data[0], "%Y%m%d%H%M")  # 문자열을 datetime 객체로 변환합니다.
        formatted_date = date.strftime("%Y-%m-%d %H:%M")  # 원하는 형식으로 날짜와 시간을 포맷팅합니다.
        stn = int(row_data[1])
        pm10 = int(row_data[2])
        # 새로운 데이터를 temp 테이블에 삽입합니다.
        cur.execute(f"INSERT INTO {schema}.{temp_table_name} (date, stn, pm10) VALUES (%s, %s, %s)", (formatted_date, stn, pm10))

    # 변경사항을 저장합니다.
    cur.connection.commit()

    # 테이블 이름 변경하기 전에 트랜잭션을 시작
    cur.execute("BEGIN")

    if table_exists:
        # 테이블 잠금
        cur.execute(f"LOCK TABLE {schema}.{table} IN ACCESS EXCLUSIVE MODE")
        # 기존 테이블 삭제
        cur.execute(f"DROP TABLE IF EXISTS {schema}.{table}")

    # 임시 테이블 이름 변경
    cur.execute(f"ALTER TABLE {schema}.{temp_table_name} RENAME TO {table}")

    # 변경사항을 저장합니다.
    cur.connection.commit()

    # Redshift 연결을 닫습니다.
    cur.close()


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
