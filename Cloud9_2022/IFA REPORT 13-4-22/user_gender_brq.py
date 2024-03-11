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

brq_c=df.select('ifa','brq_count')



brq_c_null=brq_c.groupBy('brq_count').agg(F.countDistinct('ifa').alias('ifa_numbers')).sort('ifa_numbers', ascending=False)


#For GPS

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
 |    |    |-- req_con_type: integer (nullable = true)
 |    |    |-- req_carrier_code: string (nullable = true)
 |    |    |-- req_carrier_mcc: string (nullable = true)
 |    |    |-- req_carrier_mnc: string (nullable = true)
 |    |    |-- req_carrier_name: string (nullable = true)
 |    |    |-- req_con_type_desc: string (nullable = true)
 |    |    |-- ip: string (nullable = true)
 |    |    |-- mm_carrier_name: string (nullable = true)
 |    |    |-- mm_con_type_desc: string (nullable = true)
 |    |    |-- user_agent: string (nullable = true)
 |    |    |-- brq_count: long (nullable = true)
 |    |    |-- first_seen: timestamp (nullable = true)
 |    |    |-- last_seen: timestamp (nullable = true)
 |    |    |-- duration: float (nullable = true)
 |-- app: array (nullable = true)
 |    |-- element: struct (containsNull = true)
 |    |    |-- bundle: string (nullable = true)
 |    |    |-- brq_count: long (nullable = true)
 |    |    |-- first_seen: timestamp (nullable = true)
 |    |    |-- last_seen: timestamp (nullable = true)
 |    |    |-- duration: float (nullable = true)
 |    |    |-- asn: string (nullable = true)
 |    |    |-- platform: string (nullable = true)
 |-- maxmind: array (nullable = true)
 |    |-- element: struct (containsNull = true)
 |    |    |-- geohash: string (nullable = true)
 |    |    |-- brq_count: long (nullable = true)
 |    |    |-- first_seen: timestamp (nullable = true)
 |    |    |-- last_seen: timestamp (nullable = true)
 |    |    |-- duration: float (nullable = true)
 |    |    |-- country: string (nullable = true)
 |    |    |-- state: string (nullable = true)
 |    |    |-- latitude: float (nullable = true)
 |    |    |-- longitude: float (nullable = true)
 |    |    |-- city: string (nullable = true)
 |    |    |-- state_name: string (nullable = true)
 |-- gps: array (nullable = true)
 |    |-- element: struct (containsNull = true)
 |    |    |-- geohash: string (nullable = true)
 |    |    |-- brq_count: long (nullable = true)
 |    |    |-- first_seen: timestamp (nullable = true)
 |    |    |-- last_seen: timestamp (nullable = true)
 |    |    |-- duration: float (nullable = true)
 |    |    |-- country: string (nullable = true)
 |    |    |-- state: string (nullable = true)
 |    |    |-- latitude: float (nullable = true)
 |    |    |-- longitude: float (nullable = true)
 |    |    |-- city: string (nullable = true)
 |    |    |-- state_name: string (nullable = true)
 |-- user: struct (nullable = true)
 |    |-- gender: string (nullable = true)
 |    |-- yob: integer (nullable = true)
 |    |-- age: integer (nullable = true)
 |-- brq_count: long (nullable = true)
 
 
## gps
df=spark.read.parquet('s3a://ada-prod-data/etl/data/brq/agg/agg_brq/daily/BD/2022{0301,0302,0303,0304,0305,0306,0307,0308,0309,0310,0311,0312,0313,0314,0315,0316,0317,0318,0319,0320,0321,0322,0323,0324,0325,0326,0327,0328,0329,0330}/*.parquet')

geohash=df.select('ifa','gps.country')
geohash_ifa=geohash.groupBy('country').agg(F.countDistinct('ifa').alias('ifa_numbers')).sort('ifa_numbers', ascending=False)
geohash_ifa.show(20,False)


#FOR USER
df=spark.read.parquet('s3a://ada-prod-data/etl/data/brq/agg/agg_brq/daily/BD/2022{0301,0302,0303,0304,0305,0306,0307,0308,0309,0310,0311,0312,0313,0314,0315,0316,0317,0318,0319,0320,0321,0322,0323,0324,0325,0326,0327,0328,0329,0330}/*.parquet')

gender=df.select('ifa','user.age')
gender_2=gender.groupBy('age').agg(F.countDistinct('ifa').alias('ifa_numbers')).sort('ifa_numbers', ascending=False)
