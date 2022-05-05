'''
TDMS to apache Parquet
'''

5장부터 시작, DDL dictionary, 5장에 추가되어 있는 저장 구조는 제외
6장 ㅇDML 7장 및 8장 9장 뷰를 넣는걸로 하고, 10장 서브쿼리 오늘 설명한 정도까지만
# Various Import
import os
from nptdms import TdmsFile as td
import pyarrow as pa
import pyarrow.parquet as pq


# extract path
def get_file_list():
    dir_path = "../tdms/"
    file_list = os.listdir(dir_path)
    return dir_path, file_list


dir_path, file_list = get_file_list()
print("totla %d tdms files..." % (len(file_list)))

# for all TDMS files
for file in file_list:
    tdms_file_path = dir_path + file
    tdms_file = td.read(tdms_file_path)
    
    # each channel..(CNC, Sensor..)
    for i in range(len(tdms_file)):
        # convert to table
        df = tdms_file.groups()[i].as_dataframe()
        # if DF is under 2, do not process
        if len(df) < 2:  
            continue

        Table = pa.Table.from_pandas(df)
        # set extension
        filename = os.path.splitext(file)
        pq.write_table(Table, '../parquet/' + filename[0] + '+' + tdms_file.groups()[i].name + '.parquet')

    print(file, "conversion completed")
print("conversion totally completed")
