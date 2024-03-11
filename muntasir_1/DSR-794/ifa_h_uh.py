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



df= spark.read.parquet('s3a://ada-prod-data/etl/data/brq/sub/affluence/monthly/BD/2022{08}/*.parquet')

df=df.select('ifa','final_affluence')

df=df.filter(df.final_affluence!='Low')

df=df.filter(df.final_affluence!='Mid')

df=df.filter(df.final_affluence!='Ultra High')

df=df.select('ifa')

df=df.distinct()

df=df.withColumnRenamed("ifa","Mobile Device ID")


df.coalesce(17).write.csv('s3a://ada-bd-emr/muntasir/DSR_794_JAGO_NEW/multiple/', mode='overwrite', header=True)



df=spark.read.csv('s3a://ada-bd-emr/muntasir/DSR_794_JAGO_NEW/multiple/*.csv',header=True)

count_df = df.groupBy('final_affluence').agg(F.countDistinct('ifa').alias('freq')).sort('freq', ascending = False)

count_df.show(10,0)