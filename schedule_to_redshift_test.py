from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import json
import re
import time
import logging
import pytz
import psycopg2

# 로깅 설정
logging.basicConfig(level=logging.INFO)


# Redshift 연결 함수
def get_Redshift_connection():
    conn = psycopg2.connect(dbname="", user="", password="", host="", port="5439")
    return conn.cursor(), conn


# Extract Task
@task
def extract():
    logging.info("Starting extract task")
    options = webdriver.ChromeOptions()
    options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument(
        "--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    )

    driver = webdriver.Chrome(options=options)
    url = "https://www.koreabaseball.com/Schedule/Schedule.aspx"
    driver.get(url)

    games = []

    korea_tz = pytz.timezone("Asia/Seoul")
    today = datetime.now(korea_tz).strftime("%m.%d")

    try:
        time.sleep(2)
        schedule_table = WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.CLASS_NAME, "tbl")))

        rows = schedule_table.find_elements(By.TAG_NAME, "tr")

        current_date = None
        for row in rows:
            columns = row.find_elements(By.TAG_NAME, "td")
            if len(columns) > 0:
                if "day" in columns[0].get_attribute("class"):
                    current_date = columns[0].text.strip().split("(")[0].strip()  # 요일 부분 제거
                    game_time = columns[1].text.strip()
                    teams = re.sub(r"\d+", "", columns[2].text.strip()).replace("\n", " ")
                    stadium = columns[7].text.strip()
                else:
                    game_time = columns[0].text.strip()
                    teams = re.sub(r"\d+", "", columns[1].text.strip()).replace("\n", " ")
                    stadium = columns[6].text.strip()

                if current_date == today:
                    game = {"date": current_date, "time": game_time, "teams": teams, "stadium": stadium}
                    games.append(game)
    except Exception as e:
        logging.error(f"Error processing schedule: {e}")

    driver.quit()

    json_data = json.dumps(games, ensure_ascii=False)
    logging.info(f"Extracted JSON: {json_data}")
    return json_data


# Transform Task
@task
def transform(json_data):
    logging.info("Starting transform task")
    try:
        games = json.loads(json_data)
        current_year = datetime.now().year
        for game in games:
            # 날짜 형식을 'YYYYMMDD' 형식으로 변환
            date_str = game["date"]
            date_obj = datetime.strptime(f"{current_year}.{date_str}", "%Y.%m.%d")
            game["date"] = date_obj.strftime("%Y%m%d")
        transformed_data = json.dumps(games, ensure_ascii=False)
        logging.info(f"Transformed JSON: {transformed_data}")
        return transformed_data
    except Exception as e:
        logging.error(f"Error in transform task: {e}")
        raise


# Load Task
@task
def load(transformed_data, schema, table):
    logging.info("Starting load task")
    try:
        games = json.loads(transformed_data)
        cur, conn = get_Redshift_connection()

        # 테이블이 없을 경우 생성
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {schema}.{table} (
            date VARCHAR(10),
            time VARCHAR(10),
            teams VARCHAR(50),
            stadium VARCHAR(50)
        );
        """
        logging.info(create_table_sql)
        cur.execute(create_table_sql)

        if games:
            # 임시 테이블 생성
            temp_table = f"{table}_temp"
            create_temp_table_sql = f"CREATE TEMP TABLE {temp_table} (LIKE {schema}.{table});"
            logging.info(create_temp_table_sql)
            cur.execute(create_temp_table_sql)

            # 임시 테이블에 데이터 삽입
            insert_temp_sql = f"""
            INSERT INTO {temp_table} (date, time, teams, stadium)
            VALUES {','.join(["('{}','{}','{}','{}')".format(game['date'], game['time'], game['teams'], game['stadium']) for game in games])};
            """
            logging.info(insert_temp_sql)
            cur.execute(insert_temp_sql)

            # 메인 테이블에 데이터 병합
            merge_sql = f"""
            BEGIN;
            DELETE FROM {schema}.{table}
            USING {temp_table}
            WHERE {schema}.{table}.date = {temp_table}.date
              AND {schema}.{table}.time = {temp_table}.time
              AND {schema}.{table}.teams = {temp_table}.teams;
            INSERT INTO {schema}.{table}
            SELECT * FROM {temp_table}
            ORDER BY date, time;
            END;
            """
            logging.info(merge_sql)
            cur.execute(merge_sql)
            conn.commit()
            logging.info("Load task completed successfully")
        else:
            logging.info("No games found to load.")
    except Exception as e:
        conn.rollback()
        logging.error(f"Error in load task: {e}")
        raise
    finally:
        if games:
            drop_temp_table_sql = f"DROP TABLE IF EXISTS {temp_table};"
            logging.info(drop_temp_table_sql)
            cur.execute(drop_temp_table_sql)
            conn.commit()
        cur.close()
        conn.close()


# Airflow DAG 정의
default_args = {
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
}

with DAG(
    dag_id="kbo_schedule_scraper",
    start_date=datetime(2024, 6, 12),
    schedule_interval="0 0,10 * * *",  # 매일 0시와 0시 10분에 실행
    max_active_runs=1,
    catchup=False,
    default_args=default_args,
) as dag:

    schema = "nooroneg0503"
    table = "kbo_schedule"

    extracted_data = extract()
    transformed_data = transform(extracted_data)
    load(transformed_data, schema, table)
