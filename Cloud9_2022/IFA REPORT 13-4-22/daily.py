
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
from functools import reduce
from pyspark.sql import *

#Making dataframe for 25- 35 age people with Affluence of UH,H ,MID


df=spark.read.parquet('s3a://ada-prod-data/etl/data/brq/agg/agg_brq/daily/BD/2022{0301,0302,0303,0304,0305,0306,0307,0308,0309,0310,0311,0312,0313,0314,0315,0316,0317,0318,0319,0320,0321,0322,0323,0324,0325,0326,0327,0328,0329,0330}/*.parquet')








df2=df.select('ifa','device.device_vendor','device.device_name','device.device_manufacturer','device.device_model','device.device_year_of_release','device.platform','device.major_os','device.device_category')





device_vendor=df2.select('ifa','device_vendor')




device=device_vendor.groupBy("device_vendor").agg(F.countDistinct('ifa').alias('ifa_numbers')).sort('ifa_numbers', ascending=False)




device.show(200)

>>> con_df.printSchema()
root
 |-- ifa: string (nullable = true)
 |-- req_con_type: integer (nullable = true)
 |-- req_carrier_code: string (nullable = true)
 |-- req_carrier_mcc: string (nullable = true)
 |-- req_carrier_mnc: string (nullable = true)
 |-- req_carrier_name: string (nullable = true)
 |-- req_con_type_desc: string (nullable = true)
 |-- ip: string (nullable = true)
 |-- mm_carrier_name: string (nullable = true)
 |-- mm_con_type_desc: string (nullable = true)
 |-- user_agent: string (nullable = true)
 |-- brq_count: long (nullable = true)
 |-- first_seen: timestamp (nullable = true)
 |-- last_seen: timestamp (nullable = true)
 |-- duration: float (nullable = true)
 
 


df=spark.read.parquet('s3a://ada-prod-data/etl/data/brq/agg/agg_brq/daily/BD/2022{0301,0302,0303,0304,0305,0306,0307,0308,0309,0310,0311,0312,0313,0314,0315,0316,0317,0318,0319,0320,0321,0322,0323,0324,0325,0326,0327,0328,0329,0330}/*.parquet')



con_df=df.select('ifa',explode('connection')).select('ifa','col.*')


 

device_vendor=con_df.select('ifa','mm_carrier_name')




device=device_vendor.groupBy("mm_carrier_name").agg(F.countDistinct('ifa').alias('ifa_numbers')).sort('ifa_numbers', ascending=False)




device.show(200)

