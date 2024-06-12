from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook

from datetime import datetime
from datetime import timedelta

import requests
import logging
import json


def get_Redshift_connection():
    # autocommit is False by default
    hook = PostgresHook(postgres_conn_id='redshift_dev_db')
    return hook.get_conn().cursor()

@task
def etl(schema, table, serviceKey, base_time, nx, ny):
    yesterday = datetime.now() - timedelta(days=1)
    base_date = yesterday.strftime('%Y%m%d')
    url = f"http://apis.data.go.kr/1360000/VilageFcstInfoService_2.0/getVilageFcst?serviceKey={serviceKey}&numOfRows=290&pageNo=1&dataType=json&base_date={base_date}&base_time={base_time}&nx={nx}&ny={ny}"
    response = requests.get(url)
    data = json.loads(response.text)

    if 'response' in data and 'body' in data['response'] and 'items' in data['response']['body']:
        items = data['response']['body']['items']['item']
        
        informations = dict()
        for item in items :
            cate = item['category']
            fcstTime = item['fcstDate']+item['fcstTime']
            fcstValue = item['fcstValue']
            temp = dict()
            temp[cate] = fcstValue
    
            if fcstTime not in informations.keys() :
                informations[fcstTime] = dict()
        
            informations[fcstTime][cate] = fcstValue
        
            # 데이터 변환 로직
        transformed_data = []
        for timestamp, values in informations.items():
            transformed_record = {
                'timestamp': timestamp,
                'temperature': values.get('TMP', None),
                'wind_speed': values.get('WSD', None),
                'precipitation_probability': values.get('POP', None),
                'precipitation': values.get('PCP', None)
            }
            transformed_data.append(transformed_record)


        cur = get_Redshift_connection()

        existing_records = set()
        cur.execute(f"SELECT timestamp FROM {schema}.{table};")
        for row in cur.fetchall():
            existing_records.add(row[0])

        records_to_insert = [record for record in transformed_data if record['timestamp'] not in existing_records]
        
        # 테이블 생성 (필요에 따라 수정)
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


        for record in records_to_insert:
            insert_sql = f"""
            INSERT INTO {schema}.{table} (
                timestamp, temperature, wind_speed, 
                precipitation_probability, precipitation
            ) VALUES (TO_TIMESTAMP('{record['timestamp']}', 'YYYYMMDDHH24MI'), {record['temperature']}, {record['wind_speed']}, {record['precipitation_probability']}, '{record['precipitation']}')
            """
            logging.info(insert_sql)
            try:
                cur.execute(insert_sql)
                cur.execute("COMMIT;")
            except Exception as e:
                cur.execute("ROLLBACK;")
                raise

with DAG(
    dag_id = 'Forecast_to_Redshift',
    start_date = datetime(2024,6,11), # 날짜가 미래인 경우 실행이 안됨
    schedule_interval='0 0 * * *',
    max_active_runs = 4,
    catchup = False,
    default_args = {
        'retries': 1,
        'retry_delay': timedelta(minutes=3),
    }
) as dag:
    etl("funglee0910", "jamsil", Variable.get("forecast_api_key"), '2300', '62', '126')
    etl("funglee0910", "gocheog", Variable.get("forecast_api_key"), '2300', '58', '125')
    etl("funglee0910", "munhak", Variable.get("forecast_api_key"), '2300', '54', '124')
    etl("funglee0910", "suwon", Variable.get("forecast_api_key"), '2300', '60', '121')