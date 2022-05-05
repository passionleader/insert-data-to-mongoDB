'''
2.0.4 Version DB to Dataframe (RANGE)
'''
# various import 1
from time import time
from influxdb_client import InfluxDBClient, Point, WritePrecision, WriteOptions
from influxdb_client.client.write_api import SYNCHRONOUS

# exp name
# # Before you run, You'll need to get TOKEN and make BUCKET
experiment_name = "experiment1"  # location of data will inserted to bucket
channel_name = "Sensor"  # for Tag key

# Connect to InfluxDB with Token
token = "uWEUQi6A4jvuKrgktEZHaBENiUO2uAFXbwmG7kg8ZwFMKgFTTpD8PXnWbY6BDBLUYt0jTM5bxxiJDco_0jyyKw=="  # Authorize
org = "kitech"  # organization
bucket = "TDMS"  # bucket(Database) Name
client = InfluxDBClient(url="http://localhost:8086", token=token, org=org, debug=False)

start = time()
sec = 1564113193
query0 = 'from(bucket: "TDMS_TEST")' \
         '|> range(start:-5y)' \
         '|> filter(fn: (r) => r["_measurement"] == "experiment1")' \
         '|> filter(fn: (r) => r["_field"] == "S_S_AMP" or r["_field"] == "S_R_AMP")' \
         '|> filter(fn: (r) => r["channel"] == "Sensor")' \
         '|> filter(fn: (r) => r["seconds"] >= "' + str(sec) + '" and r["seconds"] <= "1564113194")'

query = 'from(bucket: "TDMS_TEST")' \
         '|> range(start:1564113193, stop: 1564113203)' \
         '|> filter(fn: (r) => r["_measurement"] == "experiment1")' \
         '|> filter(fn: (r) => r["_field"] == "S_S_AMP")' \
         '|> filter(fn: (r) => r["channel"] == "Sensor")' \
         '|> filter(fn: (r) => exists r["_value"] )'


tqr = 'from(bucket: "TDMS_TEST")' \
         '|> range(start:-5y)' \
         '|> filter(fn: (r) => r["_measurement"] == "experiment1")' \
         '|> filter(fn: (r) => r["_field"] == "S_S_AMP")' \
         '|> filter(fn: (r) => r["channel"] == "Sensor")' \
         '|> filter(fn: (r) => exists r["_value"] )'
'''
available options
    '|> group(columns: ["_field", "host"])' \
    '|> aggregateWindow(every: 1s, fn: max)' \  >> 1초마다 데이터 하나씩만 뽑는다
    '|> top(n: 10)'  >> 각 쿼리마다 상단 열개만 출력, 4개 조건 시 총 40개의 데이터
    '|> range(start:2018-01-01T00:00:00Z, stop:now())' \
    '|> filter(fn: (r) => exists r["_value"] )'  >> Filter out null values
heavy functions
    > map(), reduce(), window(), join(), union(), pivot()
'''

query_result = client.query_api().query_data_frame(org=org, query=tqr)
print(query_result.columns)

# print(query_result["_time"])             # 정확한 Timestamp
# print(query_result["_value"])            # 해당 속성의 값(val)
# print(query_result["_field"])            # 필드 키 (칼럼명)
# print(query_result["_measurement"])      # 측정명 (실험명)
# print(query_result["channel"])           # 검색 시 사용한 태그키(채널명-CNC/Sensor..)
# print(query_result["seconds"])           # 검색시 사용한 태그키(초 단위 시간)

# 검색 후 띄울 결과만 선택
# query_result1 = query_result.drop(["result", "table", "_start", "_stop"], axis=1)
query_result1 = query_result[["channel", "_field", "_time", "_value"]]

# Result
# query_result1.to_csv('saa.csv', sep=',', na_rep='NaN')  # CSV Save
# print(query_result1)  # 시간 순 정렬 X , 데이터셋 출력 후 다른 데이터셋이 출력됨
set1 = query_result1.loc[(query_result1['channel'] == 'Sensor') & (query_result1['_field'] == 'S_R_AMP')]
set2 = query_result1.loc[(query_result1['channel'] == 'Sensor') & (query_result1['_field'] == 'S_S_AMP')]
print(set1)
print(set2)

# Close Client
client.__del__()
print("res:", time() - start)