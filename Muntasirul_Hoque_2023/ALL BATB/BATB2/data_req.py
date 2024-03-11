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


brq = spark.read.parquet('s3a://ada-prod-data/etl/data/brq/agg/agg_brq/monthly/BD/2022{04,05,06,07,08,09}/*.parquet')
brq2 = brq.select('ifa', F.explode('gps')).select('ifa', 'col.*')
brq2=brq2.select('ifa','state','city','geohash')
brq2=brq2.filter(brq2.state == 'Dhaka')





dhaka_uh = spark.read.csv('s3a://ada-bd-emr/muntasir/BATB2/UH/*.csv',header=True)


common_ifa=dhaka_uh.join(brq2,'ifa','inner')
common_ifa=common_ifa.withColumn("new_geohash", substring(col("geohash"),1,6))
common_ifa=common_ifa.drop(common_ifa.geohash)
common_ifa=common_ifa.withColumnRenamed("new_geohash","geohash")





thana_list=spark.read.csv('s3a://ada-bd-emr/muntasir/BATB/dhaka_thana/final_dhaka_thana.csv',header=True)


# thana=thana_list.select('thana')
# thana=thana.distinct()
# thana.show(100)
# thana_list=thana_list.filter(thana_list.district == 'Dhaka')






thana_ifa_uh=thana_list.join(common_ifa,'geohash','inner')

# thana_ifa_uh=thana_ifa_uh.select('ifa')

# thana_ifa_uh.count()


# df.filter(df.score.isin(l))



NE=["Cantonment","Badda","Khilgaon","Khilkhet"]


NW=["Savar","Uttara","Biman Bandar","Mirpur","Darus Salam","Mohammadpur"]

C=["Gulshan","Tejgaon Ind. Area","kafrul"]


SE=["Shahbagh","Ramna","Bangshal","Keraniganj","Motijheel","Jatrabari"]


SW=["Dhamrai","Dohar","Nawabganj","Demra","Kamrangirchar","lalbhag","Chowkbazar","Dhanmondi","Kadamtali","Kalabagan","kotwali"]





zone=thana_ifa_uh.filter(thana_ifa_uh.thana.isin(SE))

zone=zone.select('ifa')

zone=zone.distinct()

zone.count()






age=spark.read.parquet('s3a://ada-prod-data/etl/data/brq/sub/age/monthly/BD/2022{03,04}/*.parquet')
age=age.withColumnRenamed("prediction","age")



age_df=zone.join(age,'ifa','inner')





age_count=age_df.groupBy('age').agg(F.countDistinct('ifa').alias('count')).sort('count', ascending = False)
age_count.show(50)





