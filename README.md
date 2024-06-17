# KBO 야구 경기장 대시보드를 위한 Airflow DAGs

## 프로젝트 주제
- KBO 경기장 별로 경기 당일의 기상 상태를 확인하여 경기 취소 가능성을 시각적으로 확인할 수 있는 대시보드 제작
- 각 경기장 별 경기일정 데이터 수집 → 기상청 API 데이터를 활용한 우천/폭염/미세먼지/풍속 4가지 경기취소 조건별 시각화
  
## 기술적 목표
- OPEN API를 이용하여 Data Extract
- AWS Redshift를 데이터 웨어하우스로 활용
- VM machine을 사용하여 Airflow 환경 구축
- Airflow 사용하여 ETL 데이터 파이프라인 구축
- Preset을 활용한 데이터 시각화 및 대시보드 개발
  
## 기술 아키텍처
<img width="763" alt="3rd기술아키텍쳐" src="https://github.com/nooreong2/devcourse-project3/assets/116233156/3118d703-694f-498c-81b3-b1da1147ae99">

## 데이터 수집 및 전처리  
  (1) 데이터 수집  
    - KBO 야구경기정보(KBO 홈페이지 Crawling)  
    - 강수량/기온/미세먼지/풍속 (기상청API-동네예보조회>단기예보조회)  
  (2) ERD  
    - KBO 야구경기정보	  
      - KBO 당일 경기정보 (경기날짜, 시간, 팀)  
      - 경기장 (잠실,고척,인천,수원)  
    - 기상 정보   
      - 우천	강우량(mm), 강수확률  
      - 미세먼지(ug/m3)  
      - 강풍	풍속(m/s)  
      - 폭염	기온(C)  
    ![3rd 데이터테이블](https://github.com/nooreong2/devcourse-project3/assets/116233156/59c8ac21-0f10-429e-9ec4-7d4a906715e1)

  (3) 데이터 변환 - 수집된 데이터를 분석 및 시각화에 적합한 형태로 변환  
    - Pandas를 사용하여 변환 작업 수행  
    - 기상 데이터와 야구 경기 데이터를 맵핑하여 통합 데이터셋 생성  
  (4) GCE 환경 구축 - Google Cloud Platform(GCP)에 필요한 인프라(VM machine) 구성  
    - Docker Engine을 사용하여 Airflow 자동화 환경 구축  
    - Redshift 데이터베이스 구성 - AWS Redshift 클러스터를 설정하고 데이터베이스와 테이블을 생성  
    - ETL(Extract, Transform, Load) 작업을 통해 변환된 데이터를 Redshift에 적재  

## 데이터 파이프라인  
  - Airflow 사용 - Airflow를 사용하여 데이터 파이프라인 자동화  
    - DAG(Directed Acyclic Graph) 작성  
        - 데이터 수집, 변환, 적재 과정을 자동화하기 위해 Airflow DAG 작성.  
        <img width="1284" alt="3rd_Airflow DAG" src="https://github.com/nooreong2/devcourse-project3/assets/116233156/9aba8202-33b6-4539-956c-5dd3776528c7">

    - 태스크 정의  
        1. 기상청 API 호출  
        2. KBO 경기 정보 크롤링  
        3. 데이터 변환 및 Redshift 적재  
    - 스케줄링: DAG를 매일 실행되도록 설정하여 데이터 파이프라인의 자동화 및 일정 관리  

## 대시보드 구성
- 데이터 시각화  
  - Preset을 사용한 대시보드 작성 - **Preset**을 사용하여 Redshift에 적재된 데이터를 시각화  
    - 기상 정보와 KBO 경기 결과를 시각화 → 경기취소 가능성 파악  
    - 4가지 주요 경기 취소조건 관련 그래프 작성  
    - 직관적인 대시보드 레이아웃  
  ![대시보드 전체](https://github.com/nooreong2/devcourse-project3/assets/116233156/eb158cb9-ab84-432a-b68a-1613d346d984)

- 경기 취소조건별 그래프  
  (1) 우천취소  
    - 취소조건  
      - 경기 시작 3시간 전 시간당 10mm 이상  
      - 경기 시작 1시간 전 5mm 이상   
      - 경기 중 5mm 이상이 지속되는 경우  
      ![강수량그래프](https://github.com/nooreong2/devcourse-project3/assets/116233156/f1b38db9-966c-4b9d-92d1-9d58ae9f9d71)

  (2) 폭염취소  
    - 최소조건  
      - 35°C 이상인 상태가 2일 이상 지속  
      ![기온그래프](https://github.com/nooreong2/devcourse-project3/assets/116233156/25bad5de-be7c-4805-a461-8d2626ea7df4)

  (3) 풍속  
    - 취소조건  
      - 풍속 21m/s 이상  
      ![풍속그래프](https://github.com/nooreong2/devcourse-project3/assets/116233156/300992e6-98b4-4a2b-8b11-d81a4cd39344)

  (4) 미세먼지 농도  
      - PM10(미세먼지) 300μg/m³이상(2시간 이상 지속)  
      ![미세먼지그래프](https://github.com/nooreong2/devcourse-project3/assets/116233156/c8555947-b5e3-45ef-9849-00bd9104e777)

