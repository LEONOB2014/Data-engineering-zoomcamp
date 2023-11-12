#!/usr/bin/env python
# coding: utf-8

import os
import argparse
from time import time
import pandas as pd
from sqlalchemy import create_engine

def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    url = params.url

    parquet_name = 'output.parquet'

    # doenload the parquet file
    os.system(f"wget {url} -O {parquet_name}")

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    df = pd.read_parquet(parquet_name)

    pd.to_datetime(df.tpep_pickup_datetime)
    pd.to_datetime(df.tpep_dropoff_datetime)

    #print(pd.io.sql.get_schema(df, name='yellow_taxi_data', con=engine))
    # df_iter = pd.read_csv('yellow_tripdata_2021-01.parquet', itererator=True, chunksize=100000)

    df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')
    df.to_sql(name=table_name, con=engine, if_exists='append')


if __name__ == '__main__':
        
    parser = argparse.ArgumentParser(description='Ingest Parquet data to Postgres')

    # user, possword, host, port, database,
    # url of the parquet file
    parser.add_argument('--user', help='user name for postgres')
    parser.add_argument('--password', help='password for postgres')
    parser.add_argument('--host', help='host for postgres')
    parser.add_argument('--port', help='port for postgres')
    parser.add_argument('--db', help='database name for postgres')
    parser.add_argument('--table_name', help='name of the table where we will write the results to')
    parser.add_argument('--url', help='url of the parquet file')

    args = parser.parse_args()

    main(args)
