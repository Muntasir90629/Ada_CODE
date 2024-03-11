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

df=spark.read.parquet('s3a://ada-prod-data/etl/data/brq/agg/agg_brq/daily/BD/2022{0501,0502,0503,0504,0505,0506,0507}/*.parquet')

df2=df.select('ifa',explode('gps')).select('ifa','col.*')

df3=df2.select('ifa','geohash','last_seen')


from pyspark.sql.functions import date_format

df3= df3.withColumn("date", date_format('last_seen', 'dd'))

df3=df3.withColumn("new_geohash", substring(col("geohash"),1,7))

# ifa_new.show()

df3=df3.drop(df3.geohash)

df3=df3.withColumnRenamed("new_geohash","geohash")


main_airport=spark.read.csv('s3a://ada-bd-emr/muntasir/airport/airport.csv',header=True)

# main_airport=main_airport.drop(main_airport.geohash)

# main_airport=main_airport.withColumnRenamed("geohash9","geohash")

# main_airport.show(truncate=False)


main_airport=main_airport.select('name','geohash9')

main_airport=main_airport.withColumn("geohash", substring(col("geohash9"),1,6))

main_airport=main_airport.drop(main_airport.geohash9)

#2 AIRPORT DATA 

dhaka_airport=main_airport.filter(main_airport.name == 'Hazrat Shahjalal International Airport')

# dhaka_airport=main_airport.filter(main_airport.geohash == 'wh0r9ht')

ctg_airport=main_airport.filter(main_airport.name == 'Shah Amanat International Airport')


# dhaka airport visitors 

dhaka_airport_visitors=dhaka_airport.join(df3,'geohash','left')

dhaka_air_ifa=dhaka_airport_visitors.select('ifa')

# count_d=dhaka_airport_visitors.select('ifa')


# count_d.distinct.count()


ctg_airport__visitors=ctg_airport.join(df3,'geohash','left')

ctg_air_ifa=ctg_airport__visitors.select('ifa')

# count_d=ctg_airport__visitors.select('ifa')


# count_d.distinct.count()


# person who visted place two place in a day 

common_ifa=dhaka_air_ifa.join(ctg_air_ifa,'ifa','left')
