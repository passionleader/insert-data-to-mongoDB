"""
Mongo + Influx
"""

# various import
import pika
import pymongo
import sys
import pandas as pd
from time import time
import pika  # rabbitmq
import threading
from multiprocessing import Process
import orjson  # module for load json

# MongoDB client
# mongo_client = pymongo.MongoClient("mongodb://localhost:27017/")  # ERROR
mongo_client = pymongo.MongoClient(connect=False)


# global variables(for callback function)
global DB_name, exp_name, my_col, group_name
DB_name = "SubscriberERROR"
exp_name = "SubscriberERROR"
my_col = mongo_client["WrongDB"]['WrongCOL']
group_name = "SubscriberERROR"


# Connect to RabbitMQ Broker
def set_rabbit():
    # rabbitMQ client(reason of localhost - this is subscriber)
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    rabbit_ch = connection.channel()
    # set receiving queue
    rabbit_ch.queue_declare(queue='mongo-parquet')
    return rabbit_ch


def callback(ch, method, properties, body):
    global DB_name, exp_name, my_col, group_name
    # global bucket, exp_name,  my_col

    # 받은 데이터(body) 가 metadata 일 경우 수행
    if 'DB' in orjson.loads(body)[0].keys():
        metadata = orjson.loads(body)
        print("\033[97m[consuming]: ", metadata, end=" ")
        print()
        # DB setting(common)
        DB_name = metadata[0]["DB"]
        exp_name = metadata[0]["exp_name"]
        group_name = metadata[0]["group"]
        # MongoDB setting
        my_db = mongo_client[DB_name]
        my_col = my_db[exp_name]

    # metadata 없이 입력받은 경우 종료시킴(변수 미 지정된 경우)
    elif exp_name == "SubscriberERROR":
        print("\033[91m[ERROR]Queue에 Data가 남아 있습니다. 잠시 뒤에 다시 시도해 주세요")
        quit()

    # 파일 하나 끝낼때마다 종료 메시지 수신
    elif 'fin_alert' in orjson.loads(body)[0].keys():
        # 파일 하나가 수신 종료됨
        print("\033[36m\r..endfile")

    # metadata 입력 받은 뒤, type 및 mongoDB 이름이 설정된 경우, dataset 으로 간주하여 데이터를 입력한다
    else:
        # p2_mongo = Process(target=mongo_insert,  args=(body,))
        # p2_mongo.start()
        try:
            mongo_insert(body)
        except pymongo.errors.DuplicateKeyError:
            print("\033[31m\r[MONGO_ERROR]: insert error, _id duplicates", end="")


# 기술통계 계산되어 보내진 body 를 그대로 mongo 입력하는 Function
def mongo_insert(body):
    received_data = orjson.loads(body)
    chan_nel = str(received_data[0]["level_1"])
    second = str(received_data[0]["second"])
    document = {
        "_id": group_name + '+' + chan_nel + '+' + second,
    }
    received_data[0].update(document)
    my_col.insert_one(received_data[0])
    # Timestamp 기준으로 ID를 생성했기 때문에 ID(기본키)가 겹칠 가능성(DuplicateKeyError) -> 추후 modify 기능으로 구현
    # print(received_data[0].keys())
    # "group": group_name,
    # "channel": received_data[0]["level_1"],
    # "second": received_data[0]["second"],
    # "data": received_data[0][]


# rabbitMQ server setting
channel = set_rabbit()

# RabbitMQ에 parquet 큐에서 메시지를 수신한다고 알리며 "callback"을 수행
channel.basic_consume(queue='mongo-parquet',
                      auto_ack=True,
                      on_message_callback=callback)

# Consuming Messages
# callback: wait forever
print('[INFO] Waiting for messages. To exit press CTRL+  C')
channel.start_consuming()

