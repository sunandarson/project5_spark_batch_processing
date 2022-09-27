#!/usr/bin/python3

from configparser import ConfigParser
from datetime import datetime
from pyspark.sql.functions import month
from pyspark.sql import functions as func

import os
import json
from ossaudiodev import SNDCTL_SYNTH_MEMAVL
import sqlparse

import pandas as pd
import numpy as np

import connection
import conn_warehouse

if __name__ == '__main__':
    filetime = datetime.now().strftime('%Y%m%d')
    print(f"[INFO] Service ETL is Starting .....")

    #connect db warehouse
    conn_dwh, engine_dwh  = conn_warehouse.conn()
    cursor_dwh = conn_dwh.cursor()

    #connect db source
    conf = connection.config('postgresql')
    conn, engine = connection.psql_conn(conf)
    cursor = conn.cursor()

    #connect hadoop
#    conf = connection.config('hadoop')
#    client = connection.hadoop_conn(conf)

    #connect spark
    conf = connection.config('spark')
    spark = connection.spark_conn(app="ETL-Digitalskola",config=conf)

    #query extract db source
    path_query = os.getcwd()+'/query/'
    query = sqlparse.format(
        open(
            path_query+'query.sql','r'
            ).read(), strip_comments=True).strip()

    #query load db warehouse
    query_dwh = sqlparse.format(
        open(
            path_query+'dwh_design.sql','r'
            ).read(), strip_comments=True).strip()

    try:
        print(f"[INFO] Service ETL is Running .....")
        df = pd.read_sql(query, engine)

        #upload local
        path = os.getcwd()
        directory = path+'/'+'local'+'/'
        if not os.path.exists(directory):
            os.makedirs(directory)
        df.to_csv(f"{directory}dim_orders_{filetime}.csv", index=False)
        print(f"[INFO] Upload Data in LOCAL Success .....")

        #insert dwh
        cursor_dwh.execute(query_dwh)
        conn_dwh.commit()
        df.to_sql('dim_orders', engine_dwh, if_exists='append', index=False)
        print(f"[INFO] Update WDH Success .....")

        #spark processing
        sparkdf = spark.createDataFrame(df)
        sparkdf.groupBy(month('order_date')) \
                    .agg(func.round(func.sum('order_total')) \
                    .alias('total_order')) \
                    .sort(month('order_date')) \
                    .toPandas() \
                    .to_csv(f"output.csv", index=False)

        print(f"[INFO] Service ETL is Success .....")
    except:
        print(f"[INFO] Service ETL is Failed .....")
    

    