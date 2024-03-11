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


# geo=spark.read.csv('s3a://ada-bd-emr/muntasir/ROBI_MOSQUE_842/output/fullview-geofence/*.csv',header=True)

# geo=geo.select('dev_ifa')

# geo=geo.withColumnRenamed("dev_ifa","ifa")

# # geo=geo.distinct()

# # geo.count()





# affluence=spark.read.parquet('s3a://ada-prod-data/etl/data/brq/sub/affluence/monthly/BD/2022{08,09,10}/*.parquet')


# affluence=affluence.select('ifa','final_affluence')


# affluence=affluence.filter(affluence.final_affluence != 'Low')

# affluence=affluence.filter(affluence.final_affluence != 'Mid')

# affluence=affluence.filter(affluence.final_affluence != '{}')

# affluence=affluence.dropna()



# geo_aff=geo.join(affluence,"ifa","inner")

# geo_aff.printSchema()


# geo=geo_aff.select('ifa')

# geo=geo.distinct()

# geo.count()




# brq_d = spark.read.parquet('s3a://ada-prod-data/etl/data/brq/sub/device/monthly/BD/2022{08,09,10}/*.parquet')



# device=brq_d.select('ifa','device.device_name','device.price')


# device=device.filter(device.price > 500 )

# device.printSchema()






# geo_dev=geo.join(device,"ifa","inner")

# geo_dev=geo_dev.select('ifa')

# geo_dev=geo_dev.distinct()

# geo_dev.count()


# geo_dev.printSchema()


# geo_dev=geo_dev.withColumnRenamed("ifa","Mobile Device Id")

# geo_dev.count()



# geo_dev.coalesce(1).write.csv('s3a://ada-bd-emr/muntasir/Hotel/', mode='overwrite', header=True)




dev_loc=spark.read.csv('s3a://ada-bd-emr/muntasir/Hotel/part-00000-862e83a0-7b43-4a24-9f30-07d1b685effe-c000.csv',header=True)
dev_loc=dev_loc.withColumnRenamed("Mobile Device Id","ifa")



brq=spark.read.parquet('s3a://ada-prod-data/etl/data/brq/agg/agg_brq/monthly/BD/2022{08,09,10}/*.parquet')
brq2=brq.select('ifa', F.explode('gps')).select('ifa', 'col.*')
brq2=brq2.select('ifa','state','city','geohash')
brq2=brq2.filter(brq2.state == 'Dhaka')
brq2=brq2.withColumn("new_geohash", substring(col("geohash"),1,6))
brq2=brq2.drop(brq2.geohash)
loc_ifa=brq2.withColumnRenamed("new_geohash","geohash")


common_ifa=dev_loc.join(loc_ifa,'ifa','inner')

# common_ifa=common_ifa.select('ifa')

# common_ifa=common_ifa.distinct()

# common_ifa.count()







look_up=spark.read.csv('s3a://ada-bd-emr/muntasir/BATB/dhaka_thana/final_dhaka_thana.csv',header=True)

cifa=look_up.join(common_ifa,'geohash','left')




freq_beh=cifa.groupBy('thana').agg(F.countDistinct('ifa').alias('freq')).sort('freq', ascending = False)


freq_beh.show(100,truncate=False)












device1=brq.select('device.device_vendor','device.device_name','device.device_manufacturer')

device2=brq.select('device.device_model','device.device_year_of_release','device.platform','device.major_os','device.device_category')








l1=affluence.groupBy("final_affluence").agg(F.countDistinct('ifa').alias('count')).sort('count', ascending=False)
