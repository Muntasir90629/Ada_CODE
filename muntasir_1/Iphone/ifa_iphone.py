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

df=spark.read.parquet('s3a://ada-prod-data/etl/data/brq/agg/agg_brq/monthly/BD/202{106,107,108,109,010,011}/*.parquet')


# df2=df.select('ifa',explode('connection')).select('ifa','col.*')


df2=df.select('ifa','device.device_vendor','device.device_name','device.device_year_of_release','device.device_model','device.device_model','device.major_os','device.platform')



df3=df2.filter(df2.device_vendor=='Apple')


df3=df3.withColumn("year", substring(col("device_year_of_release"),0,4))


df3=df3.select('ifa','device_vendor','major_os','year')



l2=["2014","2016","2017"]



df4=df3.filter(df3.year.isin(l2))


df4=df4.select('ifa','major_os')

df4=df4.filter(~F.col('major_os').startswith('iPadOS'))
df4=df4.filter(~F.col('major_os').startswith('Android'))
df4=df4.filter(~F.col('major_os').startswith('TV'))

# df4=df4.filter(df4.major_os.startsWith("iPadOS"))

# os=df4.groupBy('major_os').agg(F.countDistinct('ifa').alias('count')).sort('count', ascending = False)

# os.show(100, False)


df5=df4.withColumn("version", substring(col("major_os"),5,4))

from pyspark.sql.types import IntegerType

data_df =df5.withColumn("version", df5["version"].cast(FloatType()))

data_df_13=data_df.filter(data_df.version <= 13.0)

data_df_13_ifa=data_df_13.select('ifa')

data_df_13_ifa=data_df_13_ifa.distinct()

data_df_13_ifa.count()


data_df_13_ifa.coalesce(1).write.csv('s3a://ada-bd-emr/muntasir/iphone_v_13/', mode='overwrite', header=True)




# os=data_df_13.groupBy('version').agg(F.countDistinct('ifa').alias('count')).sort('count', ascending = False)

# os.show(100, False)



# df4.filter(df4.year=="2014").show()


df5=df4.select('ifa')


df5=df5.distinct()


df5.coalesce(1).write.csv('s3a://ada-bd-emr/muntasir/iphone_3_month_2021/', mode='overwrite', header=True)







