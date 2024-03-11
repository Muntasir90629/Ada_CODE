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
from pyspark.sql import SparkSession

from pyspark.sql.functions import date_format


###########################################################

aff = spark.read.parquet('s3a://ada-business-insights/prod/affluence/affluence_result/BD/2023{05}/*.parquet')


aff=aff.filter(aff.affluence_level == 'high')

aff=aff.select('ifa')

aff=aff.distinct()

aff.count()



# affluence=spark.read.parquet('s3a://ada-prod-data/etl/data/brq/sub/affluence/monthly/BD/2022{01,02,03}/*.parquet')

# affluence=affluence.select('ifa','final_affluence')

# affluence=affluence.filter(affluence.final_affluence == 'Ultra High')





brq = spark.read.parquet('s3a://ada-prod-data/etl/data/brq/agg/agg_brq/monthly/BD/202305/*.parquet')




brq2 = brq.select('ifa', F.explode('gps')).select('ifa', 'col.*')
brq2=brq2.select('ifa','state','city','geohash','latitude','longitude')

li=["Dhaka","Chittagong"]



brq2=brq2.filter(brq2.state .isin(li) )



df=aff.join( brq2,'ifa','inner')


df.printSchema()

df=df.select('ifa')


df=df.distinct()

df.count()



dev= spark.read.parquet('s3a://ada-prod-data/etl/data/brq/sub/device/monthly/BD/202305/*.parquet')

dev=dev.select('ifa','device.device_name','device.device_model','device.device_manufacturer','device.device_year_of_release','device.platform','device.major_os','device.device_category','device.price','device.pricegrade')


df2=df.join(dev,'ifa','inner')





df2=df2.filter(df2.price>= 650.0)


df2.show()



df2=df2.select('ifa')


df2=df2.distinct()

df2.count()



df2=df2.withColumnRenamed("ifa","Mobile Device ID")

df2.printSchema()





df2.coalesce(1).write.csv('s3a://ada-dev/BD-DataScience/muntasir/Baluka_DSR_931/final_ifa/final2_uh/', mode='overwrite', header=True)




dev= spark.read.csv('s3a://ada-dev/BD-DataScience/muntasir/Baluka_DSR_931/final_ifa/final2_uh/*.csv',header=True)

dev.printSchema()


dev.count()

# df2.agg({'price': 'avg'}).show()

# df2.agg({'price': 'max'}).show()