"""
2.0.4 Version DB to Dataframe (RANGE)
https://pypi.org/project/influxdb-client/
"""
# various import
from time import time
from influxdb_client import InfluxDBClient, Point, WritePrecision, WriteOptions

# Authorize
# You need to get Authorize TOKEN(at serverIP:8086) and make BUCKET before
token = "uWEUQi6w=="
org = "kitech"  # organization
client = InfluxDBClient(url="http://localhost:8086", token=token, org=org, debug=False)

start = time()  # 수행 시간 측정용

bucket = "TDMS"  # bucket(Database) Name
measurement_name = "experiment1"  # experiment name
search_from, search_to = 1564113193, 1564113222,
aggregate_cycle = "1000ms"  # ms, s, m, h, d, mo, y
aggregate_func = "skew"  # Descriptive function
# unique, min, max, median, mean, sum, first, last, count
# unique, sort, skew, integral, mode, spread, stddev
# derivative, nonnegative derivative, distinct, increase

query_raw = 'from(bucket: "TDMS")' \
        '|> range(start: {}, stop: {})' \
        '|> filter(fn: (r) => r["_measurement"] == "experiment1")' \
        '|> filter(fn: (r) => r["group"] == "Sensor")' \
        '|> drop(columns: ["_start", "_stop", "_measurement", "second"])' \
        '|> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")' \
        '|> yield(name: "data_name")' \
        ''.format(search_from, search_to)

query_statistic = 'from(bucket: "TDMS")' \
        '|> range(start: {}, stop: {})' \
        '|> filter(fn: (r) => r["_measurement"] == "experiment1")' \
        '|> filter(fn: (r) => r["group"] == "Sensor")' \
        '|> aggregateWindow(every: {}, fn: {}, createEmpty: false)' \
        '|> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")'\
        '|> yield(name: "data_name")' \
        ''.format(search_from, search_to, aggregate_cycle, aggregate_func)

query_old = 'from(bucket: {})' \
        '|> range(start: {}, stop: {})' \
        '|> filter(fn: (r) => r["_measurement"] == {}})' \
        '|> filter(fn: (r) => r["group"] == "Sensor")' \
        '|> aggregateWindow(every: {}, fn: {}, createEmpty: false)' \
        '|> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")'\
        '|> yield(name: "data_name")' \
        ''.format(bucket, search_from, search_to, measurement_name, aggregate_cycle, aggregate_func)

query_result_1 = client.query_api().query_data_frame(org=org, query=query_raw)
show_result_1 = query_result_1.drop(["result", "table"], axis=1)
print(show_result_1.columns)
print(show_result_1)

# query_result_2 = client.query_api().query_data_frame(org=org, query=query_statistic)
# show_result_2 = query_result_2.drop(["result", "table", "_start", "_stop", "_measurement", "second", "group"], axis=1)
# print(show_result_2)

# 속성 개요
# Query Result Columns
# result : query result name
# table : ??
# _time : real timestamp
# _start : query start time
# _stop : query end time
# _measurement : experiment name
# group : DAQ, CNC, Sensor
# second : int(timestamp) - based on 'second'
# Others.. : dataframe's column

# Close Client
client.__del__()
print("res:", time() - start)
