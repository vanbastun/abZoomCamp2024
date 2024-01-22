#!/usr/bin/env python
# coding: utf-8 

from time import time
import pandas as pd
from sqlalchemy import create_engine

engine = create_engine('postgresql://root:root@pgdatabase/ny_taxi')

engine.connect()

source_path = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-09.csv.gz'
                      
df_iter = pd.read_csv(source_path,
                      compression='gzip', iterator=True, chunksize=100000)

df = next(df_iter)

df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)

#create a table
df.head(n=0).to_sql(name='green_taxi_data', con=engine, if_exists='replace')

#insert first 100k rows
df.to_sql(name='green_taxi_data', con=engine, if_exists='append')

# looping insert
while True:
    try: 
        t_start = time()

        df = next(df_iter)

        df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
        df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)

        df.to_sql(name='green_taxi_data', con=engine, if_exists='append')

        t_end = time()

        print(f'inserted another green chunk, took {round((t_end - t_start), 3)} seconds')
    except StopIteration:
        print("Finished ingesting green taxi data into the postgres database")
        break
