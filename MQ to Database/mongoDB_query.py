import time
import pandas as pd
from pandas import DataFrame
import pymongo
import json
import pyarrow as pa
import pyarrow.parquet as pq

start = time.time()

myclient = pymongo.MongoClient("mongodb://localhost:27017/")
mydb = myclient["TDMS"]
mycol = mydb["190726"]
print("Mongo connection time:", time.time() - start)

# query, using time(sec)
cursor = mycol.find({
    "group": "CNC",
    "sec": {
        "$gte": 1564113202,  # min
        "$lt": 1564113302  # max
    }
}, {
    "_id": 0,
    "data": 1,
})
print("query time:", time.time() - start)


result_list = list(cursor)
print("list conversion time:", time.time() - start)

# Converto list to DF using loop
dataset = DataFrame()
for i in range(len(result_list)):
    temp_df = DataFrame(result_list[i]["data"])
    dataset = pd.concat([dataset, temp_df], ignore_index=True)
    # dataset = dataset.append(temp_df, ignore_index=True)  # slower
    del[temp_df]  # flush
print("DataFrame conversion time:", time.time() - start)

# Exception
if len(dataset) == 0:
    print("[ERROR]No Data Queried. Check the Query Condition")
    quit()


# final result
print(dataset)

# select needed data
I_need_this_data = dataset.query('level_1 == "A_FR"')
I_need_this_data = I_need_this_data.set_index("100ms", drop=True)
print(I_need_this_data, ", total query time:", time.time() - start)





