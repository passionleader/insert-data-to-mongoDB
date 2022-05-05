'''
2.0.4 Version DB _ get second
쿼리 수행 시에 필요한 정보(measurement, group, second 구할 수 있는 코드)
https://pypi.org/project/influxdb-client/
'''
# various import
from time import time
from influxdb_client import InfluxDBClient, Point, WritePrecision, WriteOptions

# DB name(bucket)
bucket = "TDMS"  # bucket(Database) Name

# Influx Authorize - You need to get Authorize TOKEN(at serverIP:8086) and make BUCKET before
token = "="
org = "ktvm"  # organization
client = InfluxDBClient(url="http://12.234.896.127:8086", token=token, org=org, debug=False)


# measurement 구하는 쿼리문
def get_measurement():
	query = 'import "influxdata/influxdb/schema"' \
					'schema.measurements(bucket: "{}")' \
					''.format(bucket)
	return query


# tag keys 구하는 쿼리문
def get_tagkey():
	query = 'import "influxdata/influxdb/schema"' \
					'schema.tagKeys(bucket: "{}", predicate: (r) => true, start: -5y)' \
					''.format(bucket)
	return query


# tag value: group 구하는 쿼리문
def get_tagvalue_group():  # groups
	query = 'import "influxdata/influxdb/schema"' \
					'schema.tagValues(bucket: "{}", tag: "group", predicate: (r) => true, start: -5y)'\
					''.format(bucket)
	return query


# tag value: second 구하는 쿼리문
def get_tagvalue_second():  # second
	query = 'import "influxdata/influxdb/schema"' \
					'schema.tagValues(bucket: "{}", tag: "second", predicate: (r) => true, start: -5y)'\
					''.format(bucket)
	return query


measurement_query = get_measurement()
measurement = client.query_api().query_data_frame(org=org, query=measurement_query)
print("-----Measurement_List-----\n", measurement._value)

tagkey_query = get_tagkey()
tagkey = client.query_api().query_data_frame(org=org, query=tagkey_query)
print("-------Tagkey_List-------\n", tagkey._value)

tagvalue_group_query = get_tagvalue_group()
tagvalue_group = client.query_api().query_data_frame(org=org, query=tagvalue_group_query)
print("-----Tagvalue_group_List-----\n", tagvalue_group._value)

tagvalue_second_query = get_tagvalue_second()
tagvalue_second = client.query_api().query_data_frame(org=org, query=tagvalue_second_query)
print("-----Tagvalue_second_List-----\n", tagvalue_second._value)