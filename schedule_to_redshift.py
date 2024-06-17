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
    try:
        url = "https://search.naver.com/search.naver?where=nexearch&sm=top_hty&fbm=0&ie=utf8&query=kbo"
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        }
        response = requests.get(url, headers=headers)
        response.raise_for_status()

        soup = BeautifulSoup(response.text, "html.parser")
        event_date = datetime.now().strftime("%Y%m%d")
        games = []

        for game in soup.find_all("tr", class_=f"schedule_{event_date}"):
            try:
                game_time = game.find("td", class_="time").text
                score_td = game.find("td", class_="score")
                team_lft = score_td.find("em", class_="team_lft").contents[0].strip()
                team_rgt = score_td.find("em", class_="team_rgt").contents[0].strip()
                teams = f"{team_lft} vs {team_rgt}"
                location = game.find_all("td")[2].text.strip()
                games.append([event_date, game_time, teams, location])
            except AttributeError:
                continue

        df = pd.DataFrame(games, columns=["EventDate", "GameTime", "Teams", "Location"])
        return df.to_json(orient="records")
    except requests.RequestException as e:
        logging.error(f"Request error in extract task: {e}")
        raise
    except Exception as e:
        logging.error(f"Error in extract task: {e}")
        raise

@task
def load(json_df, schema, table):
    conn = None
    cur = None
    temp_table = f"{table}_temp"
    try:
        df = pd.read_json(json_df, orient="records")
        
        cur = get_Redshift_connection()
        conn = cur.connection

        create_main_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {schema}.{table} (
            event_date VARCHAR(10),
            game_time VARCHAR(10),
            teams VARCHAR(100),
            location VARCHAR(100)
        );
        """
        cur.execute(create_main_table_sql)

        create_temp_table_sql = f"CREATE TEMP TABLE {temp_table} (LIKE {schema}.{table});"
        cur.execute(create_temp_table_sql)

        insert_temp_sql = f"INSERT INTO {temp_table} (event_date, game_time, teams, location) VALUES (%s, %s, %s, %s)"
        values = [(row["EventDate"], row["GameTime"], row["Teams"], row["Location"]) for index, row in df.iterrows()]
        cur.executemany(insert_temp_sql, values)

        insert_main_sql = f"""
        INSERT INTO {schema}.{table} (event_date, game_time, teams, location)
        SELECT event_date, game_time, teams, location
        FROM {temp_table}
        WHERE (event_date, game_time, teams) NOT IN (SELECT event_date, game_time, teams FROM {schema}.{table});
        """
        cur.execute(insert_main_sql)
        conn.commit()
    except Exception as e:
        if conn:
            conn.rollback()
        logging.error(f"Error in load task: {e}")
        raise
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()

with DAG(
    dag_id="kbo_schedule",
    start_date=datetime(2024, 6, 12),
    schedule_interval="0 0 * * *",
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
