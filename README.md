# Redesigning-datasets-to-fit-Database (incomplete)
* Transfer TDMS File(machining dataset) to Database
* this project is related with "Measurement-of-Various-Message-Transfer-Protocols"
* purpose of this project is that finding best Database and Message trasfer protocol
---
### A TDMS File Structure
![image](https://user-images.githubusercontent.com/55945939/144701635-e391f730-3f68-4773-a2d3-4752ca1710cb.png)
---

### purpose
* Insert Sensor Data to DataBase
* find(search, query in RDB) Sensor Data by tag key(primary key)
* Tag Key: 'Second', 'Sensor Type'

### Step by Step
* Milling Machine -> TDMS File
* TDMS File -> Database


### Used DataBases
* MongoDB
* InfluxDB

### Desired find result
* Find with Calculate descriptive statistics
  * Skew, Min, Max, Mean, Varience, kurtosis



---

### Comparison of MongoDB and InfluxDB
![image](https://user-images.githubusercontent.com/55945939/144701703-ff254e3f-1f94-4922-ab1b-9d5caf43479f.png)

---

### Comparison of DB Insertion Structure 
![image](https://user-images.githubusercontent.com/55945939/144701659-d8ba32a0-11c2-483a-957e-724fc2c6a37d.png)

---

### Comparison of Querying Language
![image](https://user-images.githubusercontent.com/55945939/144701720-83e0159c-c1fb-454a-953c-293007d13848.png)

---

### Final Desicion
#### InfluxDB(with 2.0)
![image](https://user-images.githubusercontent.com/55945939/145153892-220adcf5-46fa-45a9-a36b-c9c3a2108cbc.png)
![image](https://user-images.githubusercontent.com/55945939/145152778-54f43603-80ba-486a-8f7c-1c15cd56ce89.png)

* influxDB is not fast as MongoDB(data finding) but it has other advantages.
 * InfluxDB is more optimized to monitoring real-time sensor data
 * InfluxDB support more various statistic calculating tools.
* But, when storing massive data, It is useless.


#### MongoDB


* It shown better find&insert performance
* But it doesn't have statistic calculating tools

#### Final TDMS File -> MongoDB Structure
* It is very slow to use technical statistical tools in InfluxDB, so it seems  better to calculate statistic after performing 'Data Finding' through MongoDB.
![image](https://user-images.githubusercontent.com/55945939/144701588-079b527f-ac34-4a43-8983-2c8ac2fb35a3.png)


---
### Database Server Connection
![image](https://user-images.githubusercontent.com/55945939/144702077-e8d80a9a-9588-453e-8f77-22ea5f2af73e.png)

---
### Acknowledgement
#### This study has been conducted with the support of the Korea Institute of Industrial Technology as “Machinability diagnosis And control system based on machine learning for self-optimized manufacturing system (KITECH EH-21-0020)”
