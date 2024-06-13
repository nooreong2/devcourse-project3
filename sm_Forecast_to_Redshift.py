from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago

from datetime import datetime, timedelta
import requests
import pandas as pd
import json

# 변수 정의
auth_key = Variable.get("forecast_api_key")
base_time_weather = '1130'
base_time_rain_prob = '0800'
page_no = 1  # 첫 번째 페이지
num_of_rows = 100  # 한 페이지에 100개 데이터
data_type = 'JSON'  # 데이터 타입: JSON
categories = ['T1H', 'RN1', 'WSD']  # 관심 있는 카테고리
category_rain_prob = 'POP'  # 관심 있는 카테고리 (강수확률)

# 경기장 정보
stadiums = [
    {"name": "잠실경기장", "region": "송파구", "nx": 62, "ny": 126},
    {"name": "고척경기장", "region": "구로구", "nx": 58, "ny": 125},
    {"name": "인천경기장", "region": "미추홀구", "nx": 54, "ny": 124},
    {"name": "수원경기장", "region": "장안구", "nx": 60, "ny": 121},
]

# 카테고리명 매핑
category_mapping = {
    'POP': '강수확률',
    'T1H': '기온',
    'RN1': '강수량',
    'WSD': '풍속'
}

def get_weather_data(auth_key, base_date, base_time, nx, ny, page_no=1, num_of_rows=100, data_type='JSON'):
    endpoint = 'https://apihub.kma.go.kr/api/typ02/openApi/VilageFcstInfoService_2.0/getUltraSrtFcst'
    
    params = {
        'pageNo': page_no,
        'numOfRows': num_of_rows,
        'dataType': data_type,
        'base_date': base_date,
        'base_time': base_time,
        'nx': nx,
        'ny': ny,
        'authKey': auth_key
    }
    
    # GET 요청
    response = requests.get(endpoint, params=params)
    
    # 응답을 JSON 형태로 변환
    if response.status_code == 200:
        if data_type == 'JSON':
            json_response = response.json()
            return json_response
        elif data_type == 'XML':
            return response.text
    else:
        return {"error": "Failed to fetch data", "status_code": response.status_code}

def get_weather_data_rain_prob(auth_key, base_date, base_time, nx, ny, page_no=1, num_of_rows=100, data_type='JSON'):
    endpoint = 'https://apihub.kma.go.kr/api/typ02/openApi/VilageFcstInfoService_2.0/getVilageFcst'
    
    params = {
        'pageNo': page_no,
        'numOfRows': num_of_rows,
        'dataType': data_type,
        'base_date': base_date,
        'base_time': base_time,
        'nx': nx,
        'ny': ny,
        'authKey': auth_key
    }
    
    # GET 요청
    response = requests.get(endpoint, params=params)
    
    # 응답을 JSON 형태로 변환
    if response.status_code == 200:
        if data_type == 'JSON':
            json_response = response.json()
            return json_response
        elif data_type == 'XML':
            return response.text
    else:
        return {"error": "Failed to fetch data", "status_code": response.status_code}


def parse_weather_data(weather_data, categories):
    try:
        if not weather_data:
            print("No data received")
            return None
        
        if 'response' in weather_data and 'body' in weather_data['response'] and 'items' in weather_data['response']['body']:
            items = weather_data['response']['body']['items']['item']
            
            # 카테고리별로 데이터 필터링
            filtered_items = [item for item in items if item['category'] in categories]
            
            # 강수량 값 변환
            for item in filtered_items:
                if item['category'] == 'RN1' and item['fcstValue'] == '강수없음':
                    item['fcstValue'] = 0
            
            df = pd.DataFrame(filtered_items)
            return df
        else:
            print("Unexpected data format")
            return None
    except Exception as e:
        print(f"Error processing data: {e}")
        return None


def parse_weather_data_rain_prob(weather_data, category_rain_prob):
    try:
        if not weather_data:
            print("No data received")
            return None
        
        if 'response' in weather_data and 'body' in weather_data['response'] and 'items' in weather_data['response']['body']:
            items = weather_data['response']['body']['items']['item']
            
            # 카테고리별로 데이터 필터링
            filtered_items = [item for item in items if item['category'] == category_rain_prob]
            df = pd.DataFrame(filtered_items)
            return df
        else:
            print("Unexpected data format")
            return None
    except Exception as e:
        print(f"Error processing data: {e}")
        return None
    
@task
def fetch_and_store_weather_data():
    base_date = (datetime.now() - timedelta(days=1)).strftime('%Y%m%d')

    for stadium in stadiums:
        stadium_name = stadium["name"]
        nx = stadium["nx"]
        ny = stadium["ny"]

        # 날씨 데이터 가져오기
        weather_data = get_weather_data(auth_key, base_date, base_time_weather, nx, ny, page_no, num_of_rows, data_type)
        df_weather = parse_weather_data(weather_data, categories)

        # 강수 확률 데이터 가져오기
        weather_data_rain_prob = get_weather_data_rain_prob(auth_key, base_date, base_time_rain_prob, nx, ny, page_no, num_of_rows, data_type)
        df_rain_prob = parse_weather_data_rain_prob(weather_data_rain_prob, category_rain_prob)

        # 데이터프레임 저장 및 필요한 열만 남기기
        if df_weather is not None:
            df_weather['category'] = df_weather['category'].map(category_mapping)
            df_weather = df_weather[['category', 'fcstDate', 'fcstValue']]
            df_weather = df_weather.reset_index(drop=True)
            print(f"{stadium_name} Weather DataFrame:")
            print(df_weather.head())
        else:
            print(f"No weather data retrieved or incorrect data format for {stadium_name}")
            df_weather = pd.DataFrame(columns=['category', 'fcstDate', 'fcstValue'])

        if df_rain_prob is not None:
            df_rain_prob['category'] = df_rain_prob['category'].map(category_mapping)
            df_rain_prob = df_rain_prob[['category', 'fcstDate', 'fcstValue']]
            df_rain_prob = df_rain_prob.reset_index(drop=True)
            print(f"{stadium_name} Rain Probability DataFrame:")
            print(df_rain_prob.head())
        else:
            print(f"No rain probability data retrieved or incorrect data format for {stadium_name}")
            df_rain_prob = pd.DataFrame(columns=['category', 'fcstDate', 'fcstValue'])

        # Redshift에 데이터 저장
        save_to_redshift(df_weather, 'weather_data', stadium_name)
        save_to_redshift(df_rain_prob, 'weather_data', stadium_name)

def save_to_redshift(df, table_name, stadium_name):
    schema = 'public'
    redshift_hook = PostgresHook(postgres_conn_id='redshift_dev_db')
    conn = redshift_hook.get_conn()
    cur = conn.cursor()

    # 테이블 이름 설정
    full_table_name = f"{schema}.{table_name}_{stadium_name}"

    # 테이블이 존재하는지 확인
    cur.execute(f"SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = '{schema}' AND table_name = '{table_name}_{stadium_name}');")
    table_exists = cur.fetchone()[0]

    if not table_exists:
        # 테이블 생성
        create_table_query = f"""
        CREATE TABLE {full_table_name} (
            category VARCHAR(50),
            fcstDate DATE,
            fcstValue FLOAT
        );
        """
        cur.execute(create_table_query)
        conn.commit()

    # 데이터 삽입
    for index, row in df.iterrows():
        insert_query = f"""
        INSERT INTO {full_table_name} (category, fcstDate, fcstValue)
        VALUES ('{row['category']}', '{row['fcstDate']}', {row['fcstValue']})
        """
        cur.execute(insert_query)

    conn.commit()
    cur.close()
    conn.close()

# DAG 정의
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=3),
}

with DAG(
    dag_id='weather_forecast_to_redshift',
    start_date=datetime(2024, 6, 12),
    schedule_interval='5 12 * * *',
    default_args=default_args,
    catchup=False,
) as dag:
    fetch_and_store_weather_data()
