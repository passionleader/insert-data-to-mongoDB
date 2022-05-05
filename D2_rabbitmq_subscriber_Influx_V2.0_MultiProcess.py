'''
메타데이터 수신 받고 그에 따른 설정을 통해 데이터 입력
MultiProcess
&~RECOMMENDED METHOD~&
'''
# various import
import threading
import datetime
import pandas as pd
import pika  # rabbitmq
from multiprocessing import Process
from influxdb_client import InfluxDBClient, Point, WritePrecision, WriteOptions
from influxdb_client.client.write_api import SYNCHRONOUS
import orjson  # module for load json

# Influx DB 에서  Database(bucket)은 미리 만들어져 있어야 함

# Connect to InfluxDB with Token
token = "uWEUco_0jyyKw=="  # Authorize
org = "kitech"  # organization
client = InfluxDBClient(url="http://localhost:8086", token=token)
write_api = client.write_api(write_options=SYNCHRONOUS)


# Connect to RabbitMQ Broker
def set_rabbit():
    # rabbitMQ client(reason of localhost - this is subscriber)
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    rabbit_ch = connection.channel()
    # set receiving queue
    rabbit_ch.queue_declare(queue='parquet')
    return rabbit_ch


# 콜백 함수 끝나면 초기화 되지 않을 변수들(metadata, 콜백 함수는 인자 전달 X)
global bucket, exp_name
bucket = "SubscriberERROR"
exp_name = "SubscriberERROR"


# 데이터를 입력받으면 실제 실행되는 Callback 함수
def callback(ch, method, properties, body):
    global bucket, exp_name  # Use Global variable inside of the function

    # 받은 데이터(body) 가 metadata 일 경우 수행
    if 'bucket' in orjson.loads(body)[0].keys():
        metadata = orjson.loads(body)
        print("consuming: ", metadata, end=" ")
        bucket = metadata[0]["bucket"]
        exp_name = metadata[0]["exp_name"]

    # metadata 없이 입력받은 경우 종료시킴
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
        p = Process(target=influx_insert, args=(body,))
        p.start()


# Influx 데이터가 입력되는 Function
def influx_insert(*body):

    # influx2.0 버전에서는 json 그대로 입력할 수 없다. DF로 바꾸기
    received_data = orjson.loads(*body)
    dataset = pd.json_normalize(received_data)

    # set Index(Timestamp - Datetime - Index) 아래 둘 중 뭘 써도 결과는 같지만 시간복잡도 생각
    # dataset["Timestamp"] = dataset["Timestamp"].apply(datetime.datetime.fromtimestamp)  # make it datetime
    dataset["Timestamp"] = (dataset["Timestamp"]*(10**9)).apply(int)
    dataset.set_index('Timestamp', inplace=True)

    # write
    write_api.write(
        bucket=bucket, org=org,
        data_frame_measurement_name=exp_name,
        data_frame_tag_columns=['second', 'group'],  # Tag Column
        # tags={"ch_name": channel_name},  # Tag Key(optional)
        record=dataset  # Insert Data
    )
    del [dataset]


# rabbitMQ server setting
rabbit_ch = set_rabbit()

# RabbitMQ에 parquet 큐에서 메시지를 수신한다고 알리며 "callback"을 수행
rabbit_ch.basic_consume(queue='parquet',
                        auto_ack=True,
                        on_message_callback=callback)

# Consuming Messages
# callback: wait forever
print('[INFO] Waiting for messages. To exit press CTRL+  C')
rabbit_ch.start_consuming()
