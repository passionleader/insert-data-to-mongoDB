# various import
import pika
import pymongo
import sys
import pandas as pd
from time import time
import pika
import threading
from multiprocessing import Process
from influxdb_client import InfluxDBClient, Point, WritePrecision, WriteOptions
from influxdb_client.client.write_api import SYNCHRONOUS
import orjson
import pickle
import zlib
import scipy.stats
import numpy as np


mongo_client = pymongo.MongoClient(connect=False)

# global variables
global DB_name, exp_name, my_col, group_name
DB_name = "SubscriberERROR"
exp_name = "SubscriberERROR"
group_name = "SubscriberERROR"
my_col = mongo_client["WrongDB"]['WrongCOL']

# Connect to RabbitMQ Broker
def set_rabbit():
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    rabbit_ch = connection.channel()
    rabbit_ch.queue_declare(queue='parquet')
    return rabbit_ch


def callback(ch, method, properties, body):
    global DB_name, exp_name, group_name, my_col
    data = pickle.loads(zlib.decompress(body))

    # start message and end message will be subscribed as "list type"
    if str(type(data)) == "<class 'list'>":
        # MetaData
        if 'DB' in data[0].keys():
            print("consuming: {}".format(data))
            # DB setting(common)
            DB_name = data[0]["DB"]
            exp_name = data[0]["exp_name"]
            group_name = data[0]["group_name"]
            # MongoDB setting
            my_db = mongo_client[DB_name]
            my_col = my_db[exp_name]

        # end message when a file finished
        elif 'fin_alert' in data[0].keys():
            print("..ok")

    # main data will be subscribed as a "df" type
    elif str(type(data)) == "<class 'pandas.core.frame.DataFrame'>":
        # without metadata
        if exp_name == "SubscriberERROR":
            print("[ERROR]Queue is not empty, please Run again")
            quit()
        # with metadata, regard as dataset(class: dataframe)
        else:
            mongo_insert(data)


# mongo raw insert
def mongo_insert(data):
    sec = data.iloc[0]["100ms"]//10  # can delete this and set it to 'id'!
    # sec = data.index[0]//10
    dict_data = data.to_dict(orient='records')
    try:
        document = {
            "_id": group_name + '+' + str(sec),
            "group": group_name,
            "sec": int(sec),
            "data": dict_data
        }
        my_col.insert_one(document)

    #  overlapping data -> update(push)
    except pymongo.errors.DuplicateKeyError:
        document = {"data": {"$each": dict_data}}
        my_col.update_many({"_id": group_name + '+' + str(sec)}, {"$push": document})


# rabbitMQ server setting
channel = set_rabbit()

# callback
channel.basic_consume(queue='parquet',
                      auto_ack=True,
                      on_message_callback=callback)

# Consuming Messages
# callback: wait forever
print('[INFO] Waiting for messages. To exit press CTRL+  C')
channel.start_consuming()
