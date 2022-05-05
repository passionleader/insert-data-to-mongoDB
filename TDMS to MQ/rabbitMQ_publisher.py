# various import
import re  
import os 
from time import time  
import numpy as np
import scipy.stats
import pyarrow.parquet as pq 
import pika 
from tqdm import tqdm
import pickle
import zlib

# DB name
dbname = "TDMS"

# rabbitMQserver
input("run subscriber first > input enter")
server_host = '114.70.212.103'
rabbit_account = 'admin'
rabbit_password = '12345'
queue_name = "parquet"


# load file path and file list
def get_file_list():
    dir_path = "../parquet/"
    file_list = os.listdir(dir_path)
    return dir_path, file_list


# rabbitMQ Setting
def connect_rabbit():
    try:
        credentials = pika.PlainCredentials(rabbit_account, rabbit_password)
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=server_host, credentials=credentials))
        channel = connection.channel()
        channel.queue_declare(queue=queue_name)
        print("[RabbitMQ]successfully connected to RabbitMQ server")
        return channel, connection
    except pika.exceptions.ProbableAuthenticationError:
        print("[WARNING]Access Denied: check RabbitMQ account")
        quit()
    except pika.exceptions.AMQPConnectionError:
        print("[WARNING]Access Denied: server is not running")
        quit()


# Loop through generator obj
def slice_generator(time_range):
    list = range(time_range)  
    for y in tqdm(list): 
        yield y  


# statistic
def statistic_calc(batch_df):
    statistic = batch_df.groupby('100ms').agg(
        [np.mean, min, max, np.median, np.var, scipy.stats.skew, scipy.stats.kurtosis])
    statistic = statistic.stack(level=0).reset_index()
    return statistic


# Call the file path
dir_path, file_list = get_file_list()
# make connection to rabbitMQ Server
channel, connection = connect_rabbit()

file_number = 1  

for filename in file_list:
    print("\n[%d of %d]file in progress...." % (file_number, len(file_list)))
    start = time()  

    title = re.split("[.+]", filename)
    data_group = title[-2]

    experiment_name = re.split("_", title[0])[0]

    parquet_file_path = dir_path + filename
    table = pq.read_table(parquet_file_path)
    dataset = table.to_pandas()

    dataset["Timestamp"] = (dataset["Timestamp"] * 10).apply(int) 
    dataset.rename(columns={"Timestamp": "100ms"}, inplace=True)

    # send metadata
    metaData = [{
        "DB": dbname, "exp_name": experiment_name, "group_name": data_group,
        "dataset_len": len(dataset), "current_file_num": str(file_number) + '/' + str(len(file_list))
    }]
    meta = zlib.compress(pickle.dumps(metaData))
    channel.basic_publish(exchange='', routing_key=queue_name, body=meta)
    print("[Proceeding] ", data_group, metaData)

    # send data
    df_start_sec = dataset.loc[0, ['100ms']][0]//10
    df_end_sec = dataset.loc[len(dataset) - 1, ['100ms']][0]//10
    generatorObj = slice_generator(int(df_end_sec - df_start_sec))

    # by 0.1sec
    for i in generatorObj:
        statistic_df = statistic_calc(dataset.loc[dataset['100ms']//10 == df_start_sec + i])
        # Serialize and Publish
        transferSlice = zlib.compress(pickle.dumps(statistic_df))
        channel.basic_publish(exchange='', routing_key=queue_name, body=transferSlice)

    # send end alert
    fin_alert = [{"fin_alert": ""}]
    fin_dumps = zlib.compress(pickle.dumps(fin_alert))
    channel.basic_publish(exchange='', routing_key=queue_name, body=fin_dumps)

    # print finish
    print("[INFO]", filename, "file publish completed, endtime:", time() - start)
    file_number += 1

connection.close()
input("\n[FINISH]Publish completed. press Enter to finish")


