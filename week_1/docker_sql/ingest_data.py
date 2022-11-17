#!/usr/bin/env python
# coding: utf-8

# This file gets taxi data from URL specified before inserting
# into postgres DB as specified

import os
import pandas as pd
from time import time
from sqlalchemy import create_engine
import argparse

def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    url = params.url

    file_name = 'output'

    # get file, convert to csv
    os.system(f'wget {url} -O {file_name}.parquet')
    df = pd.read_parquet(f'{file_name}.parquet')
    df.to_csv(f'{file_name}.csv', index=False)

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    df_iter = pd.read_csv(f'{file_name}.csv', iterator=True, chunksize=100000)
    df = next(df_iter)

    df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
    df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'])

    df.head(n=0).to_sql(name=table_name, con=engine,
                        if_exists='replace')

    df.to_sql(name=table_name, con=engine, if_exists='append')

    for taxi_df_chunk in df_iter:
        start_time = time()
        df = taxi_df_chunk
        df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
        df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'])

        df.to_sql(name=table_name, con=engine, if_exists='append')
        end_time = time()

        print('Inserted chunk of taxi trip data, took %.3f seconds' % (end_time - start_time))
    
    # inserting taxi zone file
    taxi_zone_url = 'https://d37ci6vzurychx.cloudfront.net/misc/taxi+_zone_lookup.csv'
    os.system(f'wget {taxi_zone_url} -O taxi_zone_lookup.csv')
    df = pd.read_csv('taxi_zone_lookup.csv')
    df.columns = [col.lower() for col in df.columns]
    df.to_sql(name='zones', con=engine, if_exists='replace')
    print('Inserted taxi zone data')



if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest CSV data to Postgres')

    # user, password, host, port, db name, table name
    # url of the parquet file
    parser.add_argument('--user', help='username for postgres')
    parser.add_argument('--password', help='password for postgres')
    parser.add_argument('--host', help='host for postgres')
    parser.add_argument('--port', help='port for postgres')
    parser.add_argument('--db', help='database name for postgres')
    parser.add_argument('--table_name',
                        help='name of the table where we will write the results to')
    parser.add_argument('--url', help='url of the parquet file')

    args = parser.parse_args()
    main(args)
