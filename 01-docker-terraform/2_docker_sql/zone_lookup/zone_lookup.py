#!/usr/bin/env python
# coding: utf-8 

from time import time
import pandas as pd
from sqlalchemy import create_engine

engine = create_engine('postgresql://root:root@pgdatabase/ny_taxi')

engine.connect()

source_path = 'https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv'
                      
df_iter = pd.read_csv(source_path,
                      iterator=True, chunksize=100000)

df = next(df_iter)

#create a table
df.head(n=0).to_sql(name='zone_lookup', con=engine, if_exists='replace')

#insert first 100k rows
df.to_sql(name='zone_lookup', con=engine, if_exists='append')

# looping insert
while True:
    try: 
        t_start = time()

        df = next(df_iter)

        df.to_sql(name='zone_lookup', con=engine, if_exists='append')

        t_end = time()

        print(f'inserted another zone chunk, took {round((t_end - t_start), 3)} seconds')
    except StopIteration:
        print("Finished ingesting zone data into the postgres database")
        break
