'''
2.0.4 Version dataframe Insert
'''
# various import 1
import datetime
from influxdb_client import InfluxDBClient, Point, WritePrecision, WriteOptions
from influxdb_client.client.write_api import SYNCHRONOUS

# various import 2
import pyarrow.parquet as pq
from tqdm import tqdm

# exp name(TEMP, Auto Set at MQTT)
experiment_name = "exp1"  # location of data will inserted to bucket
channel_name = "CNC"  # for Tag key
batch_size = 12800


# Function for loop
def slice_generator():
    batch_range = range(0, len(dataset), batch_size)  # Include only range
    for y in tqdm(batch_range):  # loop as range
        yield y  # return


# load parquet file
parquet_file_path = "../parquet/190726_125311_O0001_OrigNC+CNC.parquet"
table = pq.read_table(parquet_file_path)
dataset = table.to_pandas()

# Connect to InfluxDB with Token
token = "uWEUQi6A4jvuKrgktEZHaBENiUO2uAFXbwmG7kg8ZwFMKgFTTpD8PXnWbY6BDBLUYt0jTM5bxxiJDco_0jyyKw=="  # Authorize
org = "kitech"  # organization
bucket = "TDMS"  # bucket(Database) Name
client = InfluxDBClient(url="http://localhost:8086", token=token)
write_api = client.write_api(write_options=SYNCHRONOUS)

# set Tag key
dataset["seconds"] = dataset["Timestamp"].apply(int)  # float -> integer
dataset["channel"] = channel_name  # CNC/DAQ/Sensor..

# set Index(Timestamp - Datetime - Index)
dataset["Timestamp"] = dataset["Timestamp"].apply(datetime.datetime.fromtimestamp)  # make it datetime
dataset.set_index('Timestamp', inplace=True)

# Insert to InfluxDB 2.0
obj1 = slice_generator()
for i in obj1:
    write_api.write(
        bucket=bucket, org=org,
        data_frame_measurement_name=experiment_name,
        data_frame_tag_columns=['seconds', 'channel'],  # Tag Column
        # tags={"ch_name": channel_name},  # Tag Key(optional)
        record=dataset[i:i+batch_size]  # Insert Data
    )

# quit
write_api.__del__()
client.__del__()
