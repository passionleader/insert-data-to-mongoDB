'''
subscriber
'''
# various import
from time import time
import datetime  # timestamp -> datetime
from tqdm import tqdm_gui
import datetime
import pandas as pd
import pika  # rabbitmq
from influxdb_client import InfluxDBClient, Point, WritePrecision, WriteOptions
from influxdb_client.client.write_api import SYNCHRONOUS
import orjson  # module for load json

# Influx DB 에서  Database(bucket)은 미리 만들어져 있어야 함

# Connect to InfluxDB with Token
token = "uWEUQi6A=="  # Authorize Code
org = "kitech"  # organization
client = InfluxDBClient(url="http://localhost:8086", token=token)
write_api = client.write_api(write_options=SYNCHRONOUS)


# Connect to RabbitMQ Broker
def set_rabbit():
    # rabbitMQ client(reason of localhost - this is subscriber)
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()
    # set receiving queue
    channel.queue_declare(queue='parquet')
    return channel


# 데이터를 입력받으면 실제 실행되는 함수
def callback(ch, method, properties, body):
    global channel, exp_name, bucket  # Use Global variable inside of the function

    # 받은 데이터(body) 가 metadata일 경우 수행
    if 'exp_name' in orjson.loads(body)[0].keys():
        metadata = orjson.loads(body)
        print(metadata)
        bucket = metadata[0]["dbname"]
        exp_name = metadata[0]["exp_name"]
        channel = metadata[0]["channel"]

    # metadata 없이 입력받은 경우 종료시킴
    elif exp_name == "SubscriberERROR":
        print("[ERROR]Queue에 Data가 남아 있습니다. 잠시 뒤에 다시 시도해 주세요")
        quit()

    # metadata 입력 받은 뒤, type 및 mongoDB 이름이 설정된 경우, dataset 으로 간주하여 데이터를 입력한다
    else:
        # influx2.0 버전에서는 json 그대로 입력할 수 없다. DF로 바꾸기
        received_data = orjson.loads(body)
        dataset = pd.json_normalize(received_data)

        # set Tag key
        dataset["seconds"] = dataset["Timestamp"].apply(int)  # float -> integer
        dataset["channel"] = channel  # CNC/DAQ/Sensor..

        # set Index(Timestamp - Datetime - Index)
        dataset["Timestamp"] = dataset["Timestamp"].apply(datetime.datetime.fromtimestamp)  # make it datetime
        dataset.set_index('Timestamp', inplace=True)

        # write
        write_api.write(
            bucket=bucket, org=org,
            data_frame_measurement_name=exp_name,
            data_frame_tag_columns=['seconds', 'channel'],  # Tag Column
            # tags={"ch_name": channel_name},  # Tag Key(optional)
            record=dataset  # Insert Data
        )


# rabbitMQ server setting
channel = set_rabbit()

# 콜백 함수에서 계속 쓸 전역 변수(콜백 함수는 인자 전달 X)
global channel, exp_name, bucket
exp_name = "SubscriberERROR"

# RabbitMQ에 parquet 큐에서 메시지를 수신한다고 알리며 "callback"을 수행
channel.basic_consume(queue='parquet',
                      auto_ack=True,
                      on_message_callback=callback)

# Consuming Messages
# callback: wait forever
print('[INFO] Waiting for messages. To exit press CTRL+  C')
channel.start_consuming()
