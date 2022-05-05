"""
insert mongoDB and influxDB
daq(type: byte) is too big to transfer with NATS
"""
# various import
import re
import os
from time import time
import pyarrow.parquet as pq
from tqdm import tqdm
import asyncio
import json
import pickle
import zlib
from nats.aio.client import Client as NATS

# set DB name and Collection name
dbname = "TDMS"  # = bucket name(influxDB), collection name(mongoDB)
experiment_name = "experiment1"


# load file path and file list
def get_file_list():
    dir_path = "../parquet/"
    file_list = os.listdir(dir_path)
    return dir_path, file_list


# NATS module Loop Function - metadata
async def publish_info(data):
    nc = NATS()
    # print("current max_payload byte : ", nc.max_payload)
    await nc.connect(servers=["nats://114.70.212.103:4222"])
    await nc.publish("milling", data)
    await nc.close()


# NATS module Loop Function - main dataset
async def publish_main():
    nc = NATS()
    await nc.connect(servers=["nats://114.70.212.103:4222"])
    for i in generatorObj:
        transferSlice = zlib.compress(pickle.dumps(dataset.loc[dataset['second'] == df_start_sec + i]))
        await nc.publish("milling", transferSlice)
    await nc.close()


# Loop through generator obj
def slice_generator(time_range):
    list = range(time_range)
    for y in tqdm(list):
        yield y 


# Call the file path
dir_path, file_list = get_file_list()

file_number = 1

'''for all files'''
for filename in file_list:
    # Nats loop by asyncio
    loop = asyncio.get_event_loop()

    print("\n[%d of %d]file in progress...." % (file_number, len(file_list)))
    start = time()

    # split to group name, extension name from filename
    title = re.split("[.+]", filename)
    data_group = title[-2]

    # convert to DataFrame shape
    parquet_file_path = dir_path + filename
    table = pq.read_table(parquet_file_path)
    dataset = table.to_pandas()

    # Influx tag key: group_name, second
    dataset["group"] = data_group
    dataset["second"] = dataset["Timestamp"].apply(int)

    # # send metadata
    # metaData = [{"DB": dbname, "exp_name":experiment_name, "dataset_len": len(dataset), "current_file_num": file_number, "files_in_folder": len(file_list)}]
    # meta = dumps(metaData)
    # loop.run_until_complete(publish_info(meta))
    # print("[Proceeding] ", data_group, metaData)

    # send data
    df_start_sec = dataset.loc[0, ['second']][0]
    df_end_sec = dataset.loc[len(dataset) - 1, ['second']][0]
    generatorObj = slice_generator(df_end_sec - df_start_sec)
    loop.run_until_complete(publish_main())

    # # send 'end' message
    # fin_alert = [{"fin_alert": ""}]
    # fin_dumps = dumps(fin_alert)
    # loop.run_until_complete(publish_info(fin_dumps))

    print("[INFO]", filename, "file publish completed, endtime:", time() - start)
    file_number += 1

loop.close()
input("\n[FINISH]Publish completed. press Enter to finish")

