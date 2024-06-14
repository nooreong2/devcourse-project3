from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import requests
import pandas as pd
from bs4 import BeautifulSoup
import logging


def get_Redshift_connection():
    hook = PostgresHook(postgres_conn_id="redshift_dev_db")
    return hook.get_conn().cursor()


@task
def extract():
    logging.info("Starting extract task")
    try:
        url = "https://search.naver.com/search.naver?where=nexearch&sm=top_hty&fbm=0&ie=utf8&query=kbo"
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        }
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        logging.info("Request to Naver successful")

        soup = BeautifulSoup(response.text, "html.parser")
        today = datetime.now().strftime("%Y%m%d")
        games = []

        for game in soup.find_all("tr", class_=f"schedule_{today}"):
            try:
                time = game.find("td", class_="time").text
                score_td = game.find("td", class_="score")
                team_lft = score_td.find("em", class_="team_lft").contents[0].strip()
                team_rgt = score_td.find("em", class_="team_rgt").contents[0].strip()
                teams = f"{team_lft} vs {team_rgt}"
                location = game.find_all("td")[2].text.strip()
                games.append([today, time, teams, location])
            except AttributeError as e:
                logging.error(f"Error parsing game data: {e}")

        if not games:
            logging.warning("No games found for today")

        df = pd.DataFrame(games, columns=["Date", "Time", "Teams", "Location"])
        logging.info(f"DataFrame created successfully with {len(df)} rows")

        json_df = df.to_json(orient="records")
        logging.info(f"Extracted JSON: {json_df}")
        return json_df
    except requests.RequestException as e:
        logging.error(f"Request error in extract task: {e}")
        raise
    except Exception as e:
        logging.error(f"Error in extract task: {e}")
        raise


@task
def load(json_df, schema, table):
    logging.info("Starting load task")
    conn = None
    temp_table = None
    try:
        df = pd.read_json(json_df, orient="records")
        cur = get_Redshift_connection()
        conn = cur.connection  

        temp_table = f"{schema}.{table}_temp"
        create_temp_table_sql = f"CREATE TEMP TABLE {temp_table} (LIKE {schema}.{table});"
        logging.info(create_temp_table_sql)
        cur.execute(create_temp_table_sql)

        insert_temp_sql = f"INSERT INTO {temp_table} (date, time, teams, location) VALUES %s"
        values = [(row["Date"], row["Time"], row["Teams"], row["Location"]) for index, row in df.iterrows()]
        cur.executemany(insert_temp_sql, values)

        insert_main_sql = f"""
        INSERT INTO {schema}.{table} (date, time, teams, location)
        SELECT date, time, teams, location
        FROM {temp_table}
        WHERE (date, time, teams) NOT IN (SELECT date, time, teams FROM {schema}.{table});
        """
        logging.info(insert_main_sql)
        cur.execute(insert_main_sql)
        conn.commit()
        logging.info("Load task completed successfully")
    except Exception as e:
        if conn:
            conn.rollback()
        logging.error(f"Error in load task: {e}")
        raise
    finally:
        if conn and cur:
            if temp_table:
                drop_temp_table_sql = f"DROP TABLE IF EXISTS {temp_table};"
                logging.info(drop_temp_table_sql)
                cur.execute(drop_temp_table_sql)
            cur.close()
            conn.close()


# Airflow DAG 정의

with DAG(
    dag_id="kbo_schedule",
    start_date=datetime(2024, 6, 12),
    schedule_interval="0 0 * * *",  # 매일 0시 실행
    max_active_runs=1,
    catchup=False,
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=3),
    },
) as dag:

    schema = "nooreong0503"
    table = "kbo_schedule"

    extracted_data = extract()
    load(extracted_data, schema, table)
