from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook

from datetime import datetime, timedelta
import requests
import logging
import json


# Redshift 연결을 위한 함수
def get_Redshift_connection():
    hook = PostgresHook(postgres_conn_id="redshift_dev_db")
    return hook.get_conn().cursor()


@task
def etl(schema, table, service_key, base_time, nx, ny):
    # 어제 날짜를 구함
    yesterday = datetime.now() - timedelta(days=1)
    base_date = yesterday.strftime("%Y%m%d")

    # 기상청 API 호출 URL 생성
    url = f"http://apis.data.go.kr/1360000/VilageFcstInfoService_2.0/getVilageFcst?serviceKey={service_key}&numOfRows=290&pageNo=1&dataType=json&base_date={base_date}&base_time={base_time}&nx={nx}&ny={ny}"

    # API 요청 및 응답 데이터 파싱
    response = requests.get(url)
    data = response.json()

    # 응답 데이터에서 필요한 정보 추출
    if "response" in data and "body" in data["response"] and "items" in data["response"]["body"]:
        items = data["response"]["body"]["items"]["item"]

        # 기상 데이터를 시간별로 정리
        informations = {}
        for item in items:
            cate = item["category"]
            fcst_time = item["fcstDate"] + item["fcstTime"]
            fcst_value = item["fcstValue"]

            if fcst_time not in informations:
                informations[fcst_time] = {}

            informations[fcst_time][cate] = fcst_value

        # 데이터를 변환하여 새로운 리스트에 저장
        transformed_data = []
        for timestamp, values in informations.items():
            transformed_record = {
                "timestamp": timestamp,
                "temperature": values.get("TMP", None),
                "wind_speed": values.get("WSD", None),
                "precipitation_probability": values.get("POP", None),
                "precipitation": values.get("PCP", None),
            }
            transformed_data.append(transformed_record)

        # Redshift 연결
        cur = get_Redshift_connection()

        # 테이블 생성 SQL 문
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {schema}.{table} (
            timestamp TIMESTAMP NOT NULL,
            temperature INT,
            wind_speed FLOAT,
            precipitation_probability INT,
            precipitation VARCHAR(16)
        );
        """
        logging.info(create_table_sql)
        try:
            cur.execute(create_table_sql)
            cur.execute("COMMIT;")
        except Exception as e:
            cur.execute("ROLLBACK;")
            raise

        # 테이블 존재 여부 확인
        cur.execute(
            f"""
        SELECT EXISTS (
            SELECT 1 FROM information_schema.tables 
            WHERE table_schema = '{schema}' 
            AND table_name = '{table}'
        );
        """
        )
        table_exists = cur.fetchone()[0]

        # 기존 레코드 확인
        existing_records = set()
        if table_exists:
            cur.execute(f"SELECT timestamp FROM {schema}.{table};")
            for row in cur.fetchall():
                existing_records.add(row[0])

        # 새로운 레코드만 삽입
        records_to_insert = [record for record in transformed_data if record["timestamp"] not in existing_records]

        for record in records_to_insert:
            insert_sql = f"""
            INSERT INTO {schema}.{table} (
                timestamp, temperature, wind_speed, 
                precipitation_probability, precipitation
            ) VALUES (
                TO_TIMESTAMP('{record['timestamp']}', 'YYYYMMDDHH24MI'), 
                {record['temperature']}, 
                {record['wind_speed']}, 
                {record['precipitation_probability']}, 
                '{record['precipitation']}'
            )
            """
            logging.info(insert_sql)
            try:
                cur.execute(insert_sql)
                cur.execute("COMMIT;")
            except Exception as e:
                cur.execute("ROLLBACK;")
                raise


# Airflow DAG 정의
with DAG(
    dag_id="Forecast_to_Redshift",
    start_date=datetime(2024, 6, 11),  # DAG의 시작 날짜
    schedule_interval="0 0 * * *",  # 매일 자정에 실행
    max_active_runs=4,  # 동시에 실행 가능한 최대 DAG 수
    catchup=False,  # 과거 실행 이력 무시
    default_args={
        "retries": 1,  # 실패 시 재시도 횟수
        "retry_delay": timedelta(minutes=3),  # 재시도 간격
    },
) as dag:
    # 각 장소에 대해 ETL 태스크 정의
    etl("nooreong0503", "jamsil", Variable.get("forecast_api_key"), "2300", "62", "126")
    etl("nooreong0503", "gocheog", Variable.get("forecast_api_key"), "2300", "58", "125")
    etl("nooreong0503", "munhak", Variable.get("forecast_api_key"), "2300", "54", "124")
    etl("nooreong0503", "suwon", Variable.get("forecast_api_key"), "2300", "60", "121")
