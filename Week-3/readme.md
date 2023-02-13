## Week 3 Homework
<b><u>Important Note:</b></u> <p>You can load the data however you would like, but keep the files in .GZ Format. 
If you are using orchestration such as Airflow or Prefect do not load the data into Big Query using the orchestrator.</br> 
Stop with loading the files into a bucket. </br></br>
<u>NOTE:</u> You can use the CSV option for the GZ files when creating an External Table</br>

<b>SETUP:</b></br>
Create an external table using the fhv 2019 data. </br>
Create a table in BQ using the fhv 2019 data (do not partition or cluster this table). </br>
Data can be found here: https://github.com/DataTalksClub/nyc-tlc-data/releases/tag/fhv </p>

## Question 1:
What is the count for fhv vehicle records for year 2019?
- 65,623,481
- ✅43,244,696
- 22,978,333
- 13,942,414
<img width="1127" alt="W3Q1" src="https://user-images.githubusercontent.com/34676681/218375321-d8a1f84f-3800-4809-8fe6-faf993257fb7.png">

## Question 2:
Write a query to count the distinct number of affiliated_base_number for the entire dataset on both the tables.</br> 
What is the estimated amount of data that will be read when this query is executed on the External Table and the Table?

- 25.2 MB for the External Table and 100.87MB for the BQ Table
- 225.82 MB for the External Table and 47.60MB for the BQ Table
- 0 MB for the External Table and 0MB for the BQ Table
- ✅ 0 MB for the External Table and 317.94MB for the BQ Table 
<img width="1176" alt="W3Q2" src="https://user-images.githubusercontent.com/34676681/218375331-d53bb561-9429-482a-8eb4-c53b87f56ab8.png">


## Question 3:
How many records have both a blank (null) PUlocationID and DOlocationID in the entire dataset?
- ✅ 717,748
- 1,215,687
- 5
- 20,332

## Question 4:<img width="1176" alt="W3Q2" src="https://user-images.githubusercontent.com/34676681/218375363-797d93a6-b109-4bb2-8c2e-19b924cf1ac8.png">

What is the best strategy to optimize the table if query always filter by pickup_datetime and order by affiliated_base_number?
- Cluster on pickup_datetime Cluster on affiliated_base_number
- ✅ Partition by pickup_datetime Cluster on affiliated_base_number
- Partition by pickup_datetime Partition by affiliated_base_number
- Partition by affiliated_base_number Cluster on pickup_datetime

## Question 5:
Implement the optimized solution you chose for question 4. Write a query to retrieve the distinct affiliated_base_number between pickup_datetime 2019/03/01 and 2019/03/31 (inclusive).</br> 
Use the BQ table you created earlier in your from clause and note the estimated bytes. Now change the table in the from clause to the partitioned table you created for question 4 and note the estimated bytes processed. What are these values? Choose the answer which most closely matches.
- 12.82 MB for non-partitioned table and 647.87 MB for the partitioned table
- ✅ 647.87 MB for non-partitioned table and 23.06 MB for the partitioned table
- 582.63 MB for non-partitioned table and 0 MB for the partitioned table
- 646.25 MB for non-partitioned table and 646.25 MB for the partitioned table
<img width="1076" alt="W3Q5" src="https://user-images.githubusercontent.com/34676681/218375391-6c31908f-4255-49c6-9da4-d58c6b822dff.png">


## Question 6: 
Where is the data stored in the External Table you created?

- Big Query
- ✅ GCP Bucket
- Container Registry
- Big Table


## Question 7:
It is best practice in Big Query to always cluster your data:
- True
- ✅ False


## (Not required) Question 8:
A better format to store these files may be parquet. Create a data pipeline to download the gzip files and convert them into parquet. Upload the files to your GCP Bucket and create an External and BQ Table. 


Note: Column types for all files used in an External Table must have the same datatype. While an External Table may be created and shown in the side panel in Big Query, this will need to be validated by running a count query on the External Table to check if any errors occur. 
 
