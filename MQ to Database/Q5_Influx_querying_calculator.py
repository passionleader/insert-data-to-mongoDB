
# various import
from time import time
from influxdb_client import InfluxDBClient, Point, WritePrecision, WriteOptions


# Authorize
# You need to get Authorize TOKEN(at serverIP:8086) and make BUCKET before
token = "="
org = "kitech"  # organization
client = InfluxDBClient(url="http://12.345.234.12:8086", token=token, org=org, debug=False)

def query1():
	query = 'import "influxdata/infl










start = time()  # 수행 시간 측정용


# 검색할 내용
experiment_name = "experiment1"  # location of data will inserted to bucket
group = "Sensor"
field = ["S_R_AMP", "S_S_AMP"]
sec = [1564113193, 1564113194]  # sec(이상~미만)

# Authorize
# You need to get Authorize TOKEN(at serverIP:8086) and make BUCKET before
token = "="
org = "ktvt"  # organization
bucket = "TDMS"  # bucket(Database) Name
client = InfluxDBClient(url="http://localhost:8086", token=token, org=org, debug=False)

start = time()

query = 'from(bucket: "TDMS")' \
        '|> range(start: -5y, stop: now())' \
        '|> filter(fn: (r) => r["_measurement"] == "experiment1")' \
        '|> filter(fn: (r) => r["_field"] == "{}" or r["_field"] == "{}")' \
        '|> filter(fn: (r) => r["group"] == "{}")' \
        '|> filter(fn: (r) => r["second"] >= "{}" and r["second"] < "{}")' \
        '|> filter(fn: (r) => exists r["_value"] )' \
        '|> yield(name: "aaa")' \
        ''.format(field[0], field[1], group, sec[0], sec[1])

'''
available options
    '|> group(columns: ["_field", "host"])'
    '|> aggregateWindow(every: 1s, fn: max)'  # 1초마다 데이터 하나씩만 뽑는다
    '|> top(n: 10)'  # 각 쿼리마다 상단 열개만 출력, 4개 조건 시 총 40개의 데이터
    '|> range(start:2018-01-01T00:00:00Z, stop:now())'  # 시간 형식 예시
    '|> filter(fn: (r) => exists r["_value"] )'  # 값이 존재하는지 확인함
heavy functions
    # 시간이 오래 걸리는 함수
    > map(), reduce(), window(), join(), union(), pivot()
'''

query_result = client.query_api().query_data_frame(org=org, query=query)
print(query_result.columns)

# print(query_result["_time"])             # 정확한 Timestamp
# print(query_result["_value"])            # 해당 속성의 값(val)
# print(query_result["_field"])            # 필드 키 (칼럼명)
# print(query_result["_measurement"])      # 측정명 (실험명)
# print(query_result["channel"])           # 검색 시 사용한 태그키(채널명-CNC/Sensor..)
# print(query_result["seconds"])           # 검색시 사용한 태그키(초 단위 시간)

# 검색 후 띄울 결과만 선택
# query_result1 = query_result.drop(["result", "table", "_start", "_stop"], axis=1)
query_df = query_result[["group", "_field", "_time", "_value"]]

# 두 개의 채널에 대해 세로로 긴 데이터를 옆으로 이어붙임
# new_df = pd.merge(query_df[:len(query_df)//2], query_df[len(query_df)//2:], on=("_time", "group"))
# label = ["Time", new_df["_field_x"].iloc[1], new_df["_field_y"].iloc[1], group]

# Result
# query_result1.to_csv('saa.csv', sep=',', na_rep='NaN')  # CSV Save
# print(query_result1)  # 시간 순 정렬 X , 데이터셋 출력 후 다른 데이터셋이 출력됨

print(query_df.groupby('_field').mean())


#set1 = query_result1.loc[(query_result1['group'] == 'Sensor') & (query_result1['_field'] == 'S_R_AMP')]
#set2 = query_result1.loc[(query_result1['group'] == 'Sensor') & (query_result1['_field'] == 'S_S_AMP')]
#print(set1)
#print(set2)

# Close Client
client.__del__()
print("res:", time() - start)


