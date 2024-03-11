pkg_list=com.databricks:spark-avro_2.11:4.0.0,org.apache.hadoop:hadoop-aws:2.7.1
pyspark --packages $pkg_list --driver-memory 30G --driver-cores 5 --num-executors 29 --executor-memory 30G --executor-cores 5 --conf spark.driver.memoryOverhead=512 --conf spark.debug.maxToStringFields=100 --conf spark.driver.maxResultSize=0 --conf spark.yarn.maxAppAttempts=1 --conf s7park.ui.port=10045



from pyspark import SparkContext, SparkConf, HiveContext
from pyspark.sql.functions import *
from pyspark.sql.types import *
import pyspark.sql.functions as F
import pyspark.sql.types as T
import csv
import pandas as pd
import numpy as np
import sys
from pyspark.sql import Window
from pyspark.sql.functions import rank, col
import geohash2 as geohash
import pygeohash as pgh
from functools import reduce
from pyspark.sql import *

df=spark.read.parquet('s3a://ada-prod-data/etl/data/brq/agg/agg_brq/monthly/BD/2022{02}/*.parquet')
df2=df.select('ifa')
df2=df2.distinct()
df2.count()


|-- device: struct (nullable = true)
 |    |-- device_vendor: string (nullable = true)
 |    |-- device_name: string (nullable = true)
 |    |-- device_manufacturer: string (nullable = true)
 |    |-- device_model: string (nullable = true)
 |    |-- device_year_of_release: string (nullable = true)
 |    |-- platform: string (nullable = true)
 |    |-- major_os: string (nullable = true)
 |    |-- device_category: string (nullable = true)
 
 
df3=df.select('ifa','device.device_vendor','device.device_name','device.device_manufacturer','device.device_model','device.device_year_of_release','device.platform','device.major_os','device.device_category')



for col in df3.dtypes:
    
    c=col[0]
    
    print(c)
    
    df2_aff_H_ifa=df3.groupBy(c).agg(F.countDistinct('ifa').alias('count')).sort('count', ascending = False)
    df2_aff_H_ifa.show(20, False)
    
    
root
 |-- ifa: string (nullable = true)
 |-- device: struct (nullable = true)
 |    |-- device_vendor: string (nullable = true)
 |    |-- device_name: string (nullable = true)
 |    |-- device_manufacturer: string (nullable = true)
 |    |-- device_model: string (nullable = true)
 |    |-- device_year_of_release: string (nullable = true)
 |    |-- platform: string (nullable = true)
 |    |-- major_os: string (nullable = true)
 |    |-- device_category: string (nullable = true)
 |-- connection: array (nullable = true)
 |    |-- element: struct (containsNull = true)
 |    |    |-- mm_con_type_desc: string (nullable = true)
 |    |    |-- mm_carrier_name: string (nullable = true)
 |    |    |-- req_con_type: integer (nullable = true)
 |    |    |-- req_carrier_mnc: string (nullable = true)
 |    |    |-- req_carrier_name: string (nullable = true)
 |    |    |-- req_carrier_code: string (nullable = true)
 |    |    |-- req_carrier_mcc: string (nullable = true)
 |    |    |-- brq_count: long (nullable = true)
 |    |    |-- first_seen: timestamp (nullable = true)
 |    |    |-- last_seen: timestamp (nullable = true)
 |    |    |-- user_agent: string (nullable = true)
 |    |    |-- req_con_type_desc: string (nullable = true)
 |    |    |-- ndays: long (nullable = true)
 |-- app: array (nullable = true)
 |    |-- element: struct (containsNull = true)
 |    |    |-- bundle: string (nullable = true)
 |    |    |-- brq_count: long (nullable = true)
 |    |    |-- first_seen: timestamp (nullable = true)
 |    |    |-- last_seen: timestamp (nullable = true)
 |    |    |-- platform: string (nullable = true)
 |    |    |-- asn: string (nullable = true)
 |    |    |-- ndays: long (nullable = true)
 |-- maxmind: array (nullable = true)
 |    |-- element: struct (containsNull = true)
 |    |    |-- geohash: string (nullable = true)
 |    |    |-- brq_count: long (nullable = true)
 |    |    |-- first_seen: timestamp (nullable = true)
 |    |    |-- last_seen: timestamp (nullable = true)
 |    |    |-- longitude: float (nullable = true)
 |    |    |-- state_name: string (nullable = true)
 |    |    |-- city: string (nullable = true)
 |    |    |-- state: string (nullable = true)
 |    |    |-- latitude: float (nullable = true)
 |    |    |-- country: string (nullable = true)
 |    |    |-- ndays: long (nullable = true)
 |-- gps: array (nullable = true)
 |    |-- element: struct (containsNull = true)
 |    |    |-- geohash: string (nullable = true)
 |    |    |-- brq_count: long (nullable = true)
 |    |    |-- first_seen: timestamp (nullable = true)
 |    |    |-- last_seen: timestamp (nullable = true)
 |    |    |-- longitude: float (nullable = true)
 |    |    |-- state_name: string (nullable = true)
 |    |    |-- city: string (nullable = true)
 |    |    |-- state: string (nullable = true)
 |    |    |-- latitude: float (nullable = true)
 |    |    |-- country: string (nullable = true)
 |    |    |-- ndays: long (nullable = true)
 |-- user: struct (nullable = true)
 |    |-- gender: string (nullable = true)
 |    |-- yob: integer (nullable = true)
 |    |-- age: integer (nullable = true)
 |-- brq_count: long (nullable = true)