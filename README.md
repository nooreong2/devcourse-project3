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

## 데이터
항목	수집 데이터	수집방식	출처	URL
KBO
야구경기정보	- KBO 당일 경기정보
   (경기날짜, 시간, 팀)
- 경기장
    (잠실,고척,인천,수원)	Crawling	KBO 홈페이지>일정결과>경기일정결과	https://www.koreabaseball.com/Schedule/Schedule.aspx
우천	강우량(mm), 강수확률	기상청API	기상청API>동네예보조회>초단기예보조회	https://apihub.kma.go.kr/
미세먼지
(PM10)	PM10 (ug/m3)	기상청API	기상청API>지상관측>황사관측	https://apihub.kma.go.kr/
강풍	풍속	기상청API	기상청API>동네예보조회>단기예보조회	https://apihub.kma.go.kr/
폭염	기온(C)	기상청API	기상청API>동네예보조회>단기예보조회	https://apihub.kma.go.kr/
![3rd 데이터테이블](https://github.com/nooreong2/devcourse-project3/assets/116233156/59c8ac21-0f10-429e-9ec4-7d4a906715e1)


## 대시보드 구성
## KBO 야구경기정보	
- KBO 당일 경기정보 (경기날짜, 시간, 팀)
- 경기장 (잠실,고척,인천,수원)
## 기상 정보 
- 우천	강우량(mm), 강수확률
- 미세먼지(ug/m3)
- 강풍	풍속(m/s)
- 폭염	기온(C)
