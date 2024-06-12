import json
import time
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
        url = 'https://www.koreabaseball.com/Schedule/Schedule.aspx'
        driver.get(url)

        games = []

        current_month = datetime.now().month

        month_select = Select(WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.ID, 'ddlMonth'))
        ))

        # 드롭다운의 각 월 옵션을 반복 처리
        for option in month_select.options:
            try:
                # 월 텍스트를 datetime 객체로 변환
                option_month = datetime.strptime(option.text, '%m').month
                # 현재 월 이후의 월만 처리
                if option_month >= current_month:
                    month_select.select_by_visible_text(option.text)
                    time.sleep(2)  # WebDriverWait로 교체 고려

                    while True:
                        # 스케줄 테이블 로딩 대기
                        schedule_table = WebDriverWait(driver, 10).until(
                            EC.presence_of_element_located((By.CLASS_NAME, 'tbl'))
                        )
                        # 스케줄 테이블의 각 행 처리
                        rows = schedule_table.find_elements(By.TAG_NAME, 'tr')

                        current_date = None
                        for row in rows:
                            columns = row.find_elements(By.TAG_NAME, 'td')
                            if len(columns) > 0:
                                # 날짜, 시간, 팀, 경기장 정보 추출
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

                        # 다음 페이지로 이동할 버튼이 있는지 확인
                        next_button = driver.find_elements(By.CSS_SELECTOR, 'a.btn.next')
                        if next_button and next_button[0].is_enabled():
                            next_button[0].click()
                            time.sleep(2)  # WebDriverWait로 교체 고려
                        else:
                            break
            except Exception as e:
                print(f"Error processing month {option.text}: {e}")
                continue

    except Exception as e:
        print(f"Failed to fetch KBO schedule: {e}")
    
    finally:
        driver.quit()

    with open('kbo_schedule.csv', 'w', encoding='utf-8', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=['date', 'time', 'teams', 'stadium'])
        writer.writeheader()
        writer.writerows(games)

if __name__ == "__main__":
    fetch_kbo_schedule()
