import pyarrow.parquet as pq
import redis
import zlib
import pickle
import re
import os
from orjson import dumps
from tqdm import tqdm
from time import time


class RedisQueue(object):
    def __init__(self, name, **redis_kwargs):
        self.key = name
        self.rq = redis.Redis(**redis_kwargs)

    def size(self):
        return self.rq.llen(self.key)

    def isEmpty(self): # is it empty
        return self.size() == 0

    def put(self, element):
        self.rq.lpush(self.key, element)

    def get(self, isBlocking=False, timeout=None):
        if isBlocking:
            element = self.rq.brpop(self.key, timeout=timeout)
            element = element[1]
        else:
            element = self.rq.rpop(self.key)
        return element

    def get_without_pop(self):
        if self.isEmpty():
            return None
        element = self.rq.lindex(self.key, -1)
        return element


if __name__ == "__main__":
    dbname = "TDMS"  # = bucket name(influxDB), collection name(mongoDB)
    experiment_name = "experiment1"
    redis_Q = RedisQueue('myq', host='114.70.212.103', port=6379, db=0, password='wktmalsck')


    # load file path and file list
    def get_file_list():
        dir_path = "../parquet/"
        file_list = os.listdir(dir_path)
        return dir_path, file_list


    # Loop through generator obj
    def slice_generator(time_range):
        list = range(time_range)
        for y in tqdm(list):
            yield y


    # Call the file path
    dir_path, file_list = get_file_list()

    file_number = 1

    for filename in file_list:
        print("\n[%d of %d]file in progress...." % (file_number, len(file_list)))
        start = time()

        title = re.split("[.+]", filename)
        data_group = title[-2]

        parquet_file_path = dir_path + filename
        table = pq.read_table(parquet_file_path)
        dataset = table.to_pandas()

        dataset["group"] = data_group
        dataset["second"] = dataset["Timestamp"].apply(int)

        # send metadata (as dict)
        metaData = [
            {"DB": dbname, "exp_name": experiment_name, "dataset_len": len(dataset), "current_file_num": file_number,
             "files_in_folder": len(file_list)}]
        meta = dumps(metaData)
        redis_Q.put(zlib.compress(pickle.dumps(meta)))
        print("[Proceeding] ", data_group, metaData)

        # send data
        df_start_sec = dataset.loc[0, ['second']][0]
        df_end_sec = dataset.loc[len(dataset) - 1, ['second']][0]
        generatorObj = slice_generator(df_end_sec - df_start_sec)
        EXPIRATION_SECONDS = 600
        for i in generatorObj:
            redis_Q.put(zlib.compress(pickle.dumps(dataset.loc[dataset['second'] == df_start_sec + i])))

        # send tail data(end data)
        fin_alert = [{"fin_alert": ""}]
        fin_dumps = dumps(fin_alert)
        redis_Q.put(zlib.compress(pickle.dumps( meta)))

        print("[INFO]", filename, "file publish completed, endtime:", time() - start)
        file_number += 1

    input("\n[FINISH]Publish completed. press Enter to finish")







