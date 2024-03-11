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
from pyspark.sql.functions import date_format


df=spark.read.parquet('s3a://ada-prod-data/etl/data/brq/agg/agg_brq/daily/BD/2022{0501}/*.parquet')

df2=df.select('ifa',explode('gps')).select('ifa','col.*')

df3=df2.select('ifa','geohash','last_seen')



df3= df3.withColumn("date", date_format('last_seen', 'dd'))

df3=df3.withColumn("new_geohash", substring(col("geohash"),1,6))

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


dhaka_airport=main_airport.filter(main_airport.name == 'Hazrat Shahjalal International Airport')

dhaka_airport_visitors=dhaka_airport.join(df3,'geohash','left')

dhaka_air_ifa=dhaka_airport_visitors.select('ifa','geohash')




ctg_airport=main_airport.filter(main_airport.name == 'Shah Amanat International Airport')

ctg_airport__visitors=ctg_airport.join(df3,'geohash','left')

ctg_air_ifa=ctg_airport__visitors.select('ifa','geohash')

# ctg_air_ifa.distinct()



common_ifa=dhaka_air_ifa.join(ctg_air_ifa,'ifa','innner')

common_ifa.distinct().count()
# date_cnt=common_ifa.groupBy('date').agg(F.countDistinct('ifa').alias('ifa count')).sort(col('ifa count').desc())

# date_cnt.limit(20).show(truncate=False)



sylhet_airport=main_airport.filter(main_airport.name == 'Osmani International Airport, Sylhet')

sylhet_airport__visitors=sylhet_airport.join(df3,'geohash','left')

sylhet_air_ifa=sylhet_airport__visitors.select('ifa','geohash')



common_ifa=dhaka_air_ifa.join(sylhet_air_ifa,'ifa','inner')

common_ifa.distinct().count()