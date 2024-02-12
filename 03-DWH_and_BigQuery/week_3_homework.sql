-- Create external table based on parquet files in the bucket
CREATE OR REPLACE EXTERNAL TABLE vocal-tempo-411407.nytaxi_dataset.green_taxi_ext 
OPTIONS (format='PARQUET',
        uris = ['gs://abzoomcamp2024-green-taxi-bucket/2022/green_tripdata_2022-*.parquet']
);

-- Create materialized table
CREATE OR REPLACE TABLE vocal-tempo-411407.nytaxi_dataset.green_taxi_nonpartition
AS SELECT * FROM vocal-tempo-411407.nytaxi_dataset.green_taxi_ext;

--Q1. What is count of records for the 2022 Green Taxi Data?
SELECT count(*) FROM vocal-tempo-411407.nytaxi_dataset.green_taxi_nonpartition;

/*Q2. Write a query to count the distinct number of PULocationIDs for the entire dataset on both the tables.
What is the estimated amount of data that will be read when this query is executed on the External Table and the Table? */
SELECT count(DISTINCT PULocationID) FROM `vocal-tempo-411407.nytaxi_dataset.green_taxi_ext`;

SELECT count(DISTINCT PULocationID) FROM `vocal-tempo-411407.nytaxi_dataset.green_taxi_nonpartition`;

--Q3. How many records have a fare_amount of 0?
SELECT count(*) 
FROM vocal-tempo-411407.nytaxi_dataset.green_taxi_nonpartition
WHERE fare_amount=0;

/*Q4. What is the best strategy to make an optimized table in Big Query if your query will always 
order the results by PUlocationID and filter based on lpep_pickup_datetime? 
(Create a new table with this strategy)*/
CREATE OR REPLACE TABLE vocal-tempo-411407.nytaxi_dataset.green_taxi_partitioned
PARTITION BY DATE(lpep_pickup_datetime)
CLUSTER BY PUlocationID
AS SELECT * FROM vocal-tempo-411407.nytaxi_dataset.green_taxi_ext;

--Q5. Write a query to retrieve the distinct PULocationID between lpep_pickup_datetime 06/01/2022 and 06/30/2022 (inclusive)

SELECT DISTINCT PULocationID FROM `vocal-tempo-411407.nytaxi_dataset.green_taxi_nonpartition`
WHERE DATE(lpep_pickup_datetime) BETWEEN DATE('2022-06-01') AND DATE('2022-06-30');

SELECT DISTINCT PULocationID FROM `vocal-tempo-411407.nytaxi_dataset.green_taxi_partitioned`
WHERE DATE(lpep_pickup_datetime) BETWEEN DATE('2022-06-01') AND DATE('2022-06-30');

--Q8 
SELECT count(*) FROM vocal-tempo-411407.nytaxi_dataset.green_taxi_partitioned;
/*BigQuery stores metadata about each table, including the number of rows.
 When you run a SELECT count(*) query, BigQuery can simply return the row count stored in the metadata, 
 without needing to read and process the table data. This is why the estimated bytes read for this query is 0 bytes.*/