## Kafka producer and stream application using Confluent Cloud

## Step to run:
- Run fhv texi producer
```
chmod u+x producer_fhv.py

./producer_fhy.py config.ini
```
- Run Green texi producer

```
chmod u+x producer_green.py

./producer_green.py config.ini
```
- setting path for spark in terminal
```
export PYTHONPATH="${SPARK_HOME}/python/:$PYTHONPATH"
export PYTHONPATH="${SPARK_HOME}/python/lib/py4j-0.10.9.5-src.zip:$PYTHONPATH"
```
- Now start jupyter Notebook 
```
Stream_notebook.ipynb
```


## Week 6 Homework Solution


### Question 1: 

**Please select the statements that are correct**

- ✅ Kafka Node is responsible to store topics
- ✅ Zookeeper is removed form Kafka cluster starting from version 4.0
- ✅ Retention configuration ensures the messages not get lost over specific period of time.
- ✅ Group-Id ensures the messages are distributed to associated consumers


### Question 2: 

**Please select the Kafka concepts that support reliability and availability**

- ✅ Topic Replication
- ✅ Topic Paritioning
- Consumer Group Id
- Ack All



### Question 3: 

**Please select the Kafka concepts that support scaling**  

- ✅ Topic Replication
- ✅ Topic Paritioning
- ✅ Consumer Group Id
- Ack All


### Question 4: 

**Please select the attributes that are good candidates for partitioning key. 
Consider cardinality of the field you have selected and scaling aspects of your application**  

- payment_type
- ✅ vendor_id
- passenger_count
- total_amount
- ✅ tpep_pickup_datetime
- ✅ tpep_dropoff_datetime


### Question 5: 

**Which configurations below should be provided for Kafka Consumer but not needed for Kafka Producer**

- ✅ Deserializer Configuration
- Topics Subscription
- Bootstrap Server
- ✅ Group-Id
- Offset
- Cluster Key and Cluster-Secret


### Question 6:

Please implement a streaming application, for finding out popularity of PUlocationID across green and fhv trip datasets.
Please use the datasets [fhv_tripdata_2019-01.csv.gz](https://github.com/DataTalksClub/nyc-tlc-data/releases/tag/fhv) 
and [green_tripdata_2019-01.csv.gz](https://github.com/DataTalksClub/nyc-tlc-data/releases/tag/green)

PS: If you encounter memory related issue, you can use the smaller portion of these two datasets as well, 
it is not necessary to find exact number in the  question.

Your code should include following
1. Producer that reads csv files and publish rides in corresponding kafka topics (such as rides_green, rides_fhv)
2. Pyspark-streaming-application that reads two kafka topics
   and writes both of them in topic rides_all and apply aggregations to find most popular pickup location.

✅Q6 Solution
## FHV texi topic producer message
<img width="1382" alt="Screenshot 2023-03-14 at 5 03 17 PM" src="https://user-images.githubusercontent.com/34676681/225029106-dd7f6b8f-0c13-4134-9a14-df8bb2cd3713.png">


## Green texi topic producer message
<img width="1390" alt="Screenshot 2023-03-14 at 5 05 15 PM" src="https://user-images.githubusercontent.com/34676681/225027467-8233f4f2-ea63-49a7-9c56-d54d684237a1.png">

## Aggregations texi topic producer message
<img width="1400" alt="Screenshot 2023-03-14 at 5 02 39 PM" src="https://user-images.githubusercontent.com/34676681/225027456-4516e71c-96c9-4cb5-bf3a-c51baa267b7e.png">

## Most frequent LocationID is 74
```
{"LocationID":74, "Borough": "Manhattan", "Zone": "East Harlem North")
```

<img width="1109" alt="Screenshot 2023-03-14 at 7 48 14 PM" src="https://user-images.githubusercontent.com/34676681/225030199-2568aab6-5a60-4f0c-9488-e1a5a72d12ef.png">

