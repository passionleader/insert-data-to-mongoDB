'''
2.0.4 Version Prototype(basic)
'''
import datetime
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS

# You can generate a Token from the "Tokens Tab" in the UI
token = "uWEUQi6A4jvuKrgktEZHaBENiUO2uAFXbwmG7kg8ZwFMKgFTTpD8PXnWbY6BDBLUYt0jTM5bxxiJDco_0jyyKw=="
org = "kitech"
bucket = "TDMS"

# Client Setting
client = InfluxDBClient(url="http://localhost:8086", token=token)
write_api = client.write_api(write_options=SYNCHRONOUS)

# WAY1_Use InfluxDB Line Protocol to write data
data = "mem,host=host1 used_percent=23.43234543"
write_api.write(bucket, org, data)
'''
# WAY2_Use a Data Point to write data
point = Point("mem")\
  .tag("host", "host1")\
  .field("used_percent", 23.43234543)\
  .time(datetime.utcnow(), WritePrecision.NS)
write_api.write(bucket, org, point)
'''
# WAY3_Use a Batch Sequence to write data
sequence = ["mem,host=host1 used_percent=23.43234543",
            "mem,host=host1 available_percent=15.856523"]
write_api.write(bucket, org, sequence)
'''
# Query
query = f'from(bucket: \\"{bucket}\\") |> range(start: -1h)'
tables = client.query_api().query(query, org=org)
'''