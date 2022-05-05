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
from influxdb_client import InfluxDBClient, Point, WritePrecision, WriteOptions
from influxdb_client.client.write_api import SYNCHRONOUS
import orjson  # module for load json

# MongoDB client
# mongo_client = pymongo.MongoClient("mongodb://localhost:27017/")  # ERROR
mongo_client = pymongo.MongoClient(connect=False)

# InfluxDB Client with Token
org = "kitech"  # organization
token = "uWEUQi6A4iJDco_0jyyKw=="
influx_client = InfluxDBClient(url="http://localhost:8086", token=token)
influx_write_api = influx_client.write_api(write_options=SYNCHRONOUS)  # write_options=SYNCHRONOUS

# global variables(for callback function)
global DB_name, exp_name, my_col
DB_name = "SubscriberERROR"
exp_name = "SubscriberERROR"
my_col = mongo_client["WrongDB"]['WrongCOL']


# Connect to RabbitMQ Broker
def set_rabbit():
    # rabbitMQ client(reason of localhost - this is subscriber)
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    rabbit_ch = connection.channel()
    # set receiving queue
    rabbit_ch.queue_declare(queue='parquet')
    return rabbit_ch


def callback(ch, method, properties,body):
    global DB_name, exp_name, my_col
    # global bucket, exp_name,  my_col

    # 받은 데이터(body) 가 metadata 일 경우 수행
    if 'DB' in orjson.loads(body)[0].keys():
        metadata = orjson.loads(body)
        print("consuming: ", metadata, end=" ")
        # DB setting(common)
        DB_name = metadata[0]["DB"]
        exp_name = metadata[0]["exp_name"]
        # MongoDB setting
        my_db = mongo_client[DB_name]
        my_col = my_db[exp_name]

    # metadata 없이 입력받은 경우 종료시킴(변수 미 지정된 경우)
    elif exp_name == "SubscriberERROR":
        print("[ERROR]Queue에 Data가 남아 있습니다. 잠시 뒤에 다시 시도해 주세요")
        quit()

    # 파일 하나 끝낼때마다 종료 메시지 수신
    elif 'fin_alert' in orjson.loads(body)[0].keys():
        # 파일 하나가 수신 종료됨
        print("..ok")

    # metadata 입력 받은 뒤, type 및 mongoDB 이름이 설정된 경우, dataset 으로 간주하여 데이터를 입력한다
    else:
        # 인자로 튜플로 감싸 보내서 Unpack 하지 못하게끔 하기. 프로세서를 하나 만들었기 때문에 대기하지 않고 다음 데이터 불러옴(병렬처리)
        # 따라서 Mongo 혹은 Influx 삽입 오류 나도 나머지 하나는 정상 처리됨
        p1_influx = Process(target=influx_insert, args=(body,))
        p2_mongo = Process(target=mongo_insert,  args=(body,))
        p1_influx.start()
        p2_mongo.start()
        # mongo_insert(body)


# Influx 데이터가 입력되는 Function
def influx_insert(*body):
    received_data = orjson.loads(*body)
    dataset = pd.json_normalize(received_data)

    dataset["Timestamp"] = (dataset["Timestamp"] * (10 ** 9)).apply(int)
    dataset.set_index('Timestamp', inplace=True)

    # write data(main data)
    influx_write_api.write(
        bucket=DB_name, org=org,
        data_frame_measurement_name=exp_name,
        data_frame_tag_columns=['second', 'group'],  # Tag Column
        record=dataset  # Insert Data
    )

    # monitoring data(모니터링전용 bucket, 모니터링 후 1시간 뒤 자동 제거됨
    # 모니터링 전용이므로 데이터프레임의 size 만 들어감(실제 데이터의 1초마다 그래프 생성)
    influx_write_api.write("monitoring", org, [{
        "measurement": "INPUT_DATA_SIZE",
        "fields": {dataset['group'].iloc[0]: int(sys.getsizeof(dataset))},  # example data
        "time": int(time() * (10 ** 9))
    }])
    del [dataset]


# mongo raw 데이터가 입력되는 Function
def mongo_insert(*body):
    received_data = orjson.loads(*body)
    key = received_data[0]["second"]
    document = {
        #"_id": received_data[0]["group"] + '+' + str(key),
        "group" : received_data[0]["group"],
        "second" : key,
        "data": received_data
    }
    my_col.insert_one(document)


# rabbitMQ server setting
channel = set_rabbit()

# RabbitMQ에 parquet 큐에서 메시지를 수신한다고 알리며 "callback"을 수행
channel.basic_consume(queue='parquet',
                      auto_ack=True,
                      on_message_callback=callback)

# Consuming Messages
# callback: wait forever
print('[INFO] Waiting for messages. To exit press CTRL+  C')
channel.start_consuming()

