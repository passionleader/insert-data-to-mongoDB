'''
2.0.4 Version DB to element query(GARBAGE)
반복문을 통해 쿼리 결과 하나하나 append. 제일 기본적인 Prototpe
'''
# various import
from time import time
from influxdb_client import InfluxDBClient, Point, WritePrecision, WriteOptions

# Authorize and Connect
# You need to get Authorize TOKEN(at serverIP:8086) and make BUCKET before
token = "uj=="
org = "kitech"  # organization
bucket = "TDMS"  # bucket(Database) Name
client = InfluxDBClient(url="http://localhost:8086", token=token, org=org, debug=False)

query_api = client.query_api()

query = 'from(bucket: "TDMS")' \
				'|> range(start: -5y, stop: now())' \
				'|> filter(fn: (r) => r["_measurement"] == "experiment1")' \
				'|> filter(fn: (r) => r["_field"] == "S_R_AMP" or r["_field"] == "S_S_AMP")' \
				'|> filter(fn: (r) => r["group"] == "Sensor")' \
				'|> filter(fn: (r) => r["second"] >= "1564113193" and r["second"] < "1564113194")' \
				'|> filter(fn: (r) => exists r["_value"] )' \
				'|> yield(name: "aaa")' \
				''


start = time()

# method1: query one by one, list append
result = client.query_api().query(org=org, query=query)
print(result)
results = ["time", "group", "channel", "value"]

for table in result:
	for record in table.records:
		results.append([record.get_time(), record.get_measurement(), record.get_field(), record.get_value()])
print(results)

# Close Client
client.__del__()
print("res:", time() - start)
