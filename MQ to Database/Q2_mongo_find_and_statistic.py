# Mongo로부터 꺼내기
# statistic included
# statistic calculate

import time
import pandas as pd
from pandas import DataFrame
import pymongo
import numpy as np
import scipy.stats
import json
import pyarrow as pa  # parquet 처리용
import pyarrow.parquet as pq

start = time.time()

# connect
myclient = pymongo.MongoClient("mongodb://localhost:27017/")
mydb = myclient["TDMS"]
mycol = mydb["experiment1"]
print("Mongo 연결 시간:", time.time() - start)

# query
cursor = mycol.find({
    "group": "CNC",
    "sec": {
        "$gte": 1564113195,  # min
        "$lt": 1564113296  # max
    }
}, {
    "_id": 0,
    "data": 1,
})

# result
print("검색 조건 요청까지 걸리는 시간:", time.time() - start)
result_list = list(cursor)
print("리스트로 변환 시간:", time.time() - start)


# 반복문을 통해 데이터프레임으 로 바꾸기
dataset = DataFrame()
for i in range(len(result_list)):
    temp_df = DataFrame(result_list[i]["data"])
    dataset = pd.concat([dataset, temp_df], ignore_index=True)
    del[temp_df]  # flush
print("DataFrame 변환까지 누적시간:", time.time() - start)

print(dataset)

print(dataset["level_1"].unique())
new = dataset["level_1"] == 'A_FR'
print(dataset[new])

# # statistic 최종 결과
# # statistic calc
# statistic = dataset.groupby("100ms").agg(
#     [np.mean, min, max, np.median, np.var, scipy.stats.skew, scipy.stats.kurtosis])
# # index cleaning
# statistic_df = statistic.stack(level=1).reset_index()
#
# print(statistic_df)
