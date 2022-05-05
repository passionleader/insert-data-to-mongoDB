'''
2.0.4 Version DB query
'''
# various import 1
import datetime
from influxdb_client import InfluxDBClient, Point, WritePrecision, WriteOptions
from influxdb_client.client.write_api import SYNCHRONOUS
import pandas as pd

# exp name
# # Before you run, You'll need to get TOKEN and make BUCKET
experiment_name = "exp1"  # location of data will inserted to bucket
channel_name = "CNC"  # for Tag key

# Connect to InfluxDB with Token
token = "uWEUQi6A4jvuKrgktEZHaBENiUO2uAFXbwmG7kg8ZwFMKgFTTpD8PXnWbY6BDBLUYt0jTM5bxxiJDco_0jyyKw=="  # Authorize
org = "kitech"   # organization
bucket = "TDMS"  # bucket(Database) Name

client = InfluxDBClient(url="http://localhost:8086", token=token)
query_api = client.query_api()

querying = 'from(bucket: "TDMS")\
|> range(start: 2019-01-01T00:00:00Z, stop:now()) \
|> filter(fn: (r) => r["_measurement"] == "exp1") \
|> filter(fn: (r) => r["channel"] == "Sensor")   \
|> filter(fn: (r) => r["seconds"] == "1564113192") \
|> unique() \
|> yield(name: "unique")'


result = client.query_api().query(org=org, query=querying)
result_list = []
print(result)
for table in result:
    print(table)
    for record in table.records:
        result_list.append((record.get_value()))
print(result_list)