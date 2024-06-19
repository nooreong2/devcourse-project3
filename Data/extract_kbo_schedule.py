import re
import csv
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.support import expected_conditions as EC

def fetch_kbo_schedule():
    options = webdriver.ChromeOptions()
    options.add_argument('--headless')  # 헤드리스 모드 설정
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')

    driver = webdriver.Chrome(options=options)

    try:
        driver.get('https://www.koreabaseball.com/Schedule/Schedule.aspx')

        games = []
        current_month = datetime.now().month

        month_select = Select(WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.ID, 'ddlMonth'))
        ))

        for option in month_select.options:
            try:
                option_month = datetime.strptime(option.text, '%m').month
                if option_month >= current_month:
                    month_select.select_by_visible_text(option.text)
                    WebDriverWait(driver, 10).until(
                        EC.presence_of_element_located((By.CLASS_NAME, 'tbl'))
                    )

                    while True:
                        schedule_table = WebDriverWait(driver, 10).until(
                            EC.presence_of_element_located((By.CLASS_NAME, 'tbl'))
                        )
                        rows = schedule_table.find_elements(By.TAG_NAME, 'tr')

                        current_date = None
                        for row in rows:
                            columns = row.find_elements(By.TAG_NAME, 'td')
                            if len(columns) > 0:
                                if 'day' in columns[0].get_attribute('class'):
                                    current_date = columns[0].text.strip()
                                    game_time = columns[1].text.strip()
                                    teams = re.sub(r'\d+', '', columns[2].text.strip()).replace('\n', ' ')
                                    stadium = columns[7].text.strip()
                                else:
                                    game_time = columns[0].text.strip()
                                    teams = re.sub(r'\d+', '', columns[1].text.strip()).replace('\n', ' ')
                                    stadium = columns[6].text.strip()

                                if current_date:
                                    game = {
                                        'date': current_date,
                                        'time': game_time,
                                        'teams': teams,
                                        'stadium': stadium
                                    }
                                    games.append(game)

                        next_button = driver.find_elements(By.CSS_SELECTOR, 'a.btn.next')
                        if next_button and next_button[0].is_enabled():
                            next_button[0].click()
                            WebDriverWait(driver, 10).until(
                                EC.presence_of_element_located((By.CLASS_NAME, 'tbl'))
                            )
                        else:
                            break
            except:
                continue

    finally:
        driver.quit()

    with open('kbo_schedule.csv', 'w', encoding='utf-8', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=['date', 'time', 'teams', 'stadium'])
        writer.writeheader()
        writer.writerows(games)

if __name__ == "__main__":
    fetch_kbo_schedule()