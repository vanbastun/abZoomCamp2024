#!/usr/bin/env python
# coding: utf-8 

from time import time
import pandas as pd
from sqlalchemy import create_engine

engine = create_engine('postgresql://root:root@pgdatabase/ny_taxi')

engine.connect()

source_path = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-01.csv.gz'
                      
df_iter = pd.read_csv(source_path,
                      compression='gzip', iterator=True, chunksize=100000)

df = next(df_iter)

df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

#create a table
df.head(n=0).to_sql(name='yellow_taxi_data', con=engine, if_exists='replace')

#insert first 100k rows
df.to_sql(name='yellow_taxi_data', con=engine, if_exists='append')

# looping insert
while True:
    try: 
        t_start = time()

        df = next(df_iter)

        df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
        df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

        df.to_sql(name='yellow_taxi_data', con=engine, if_exists='append')

        t_end = time()

        print(f'inserted another chunk, took {round((t_end - t_start), 3)} seconds')
    except StopIteration:
        print("Finished ingesting data into the postgres database")
        break
