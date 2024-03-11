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
import pygeohash as pghS
from functools import reduce
from pyspark.sql import *


# dhaka_uh = spark.read.csv('s3a://ada-bd-emr/muntasir/BATB/UH/*.csv',header=True)


zone = spark.read.csv('s3a://ada-bd-emr/muntasir/BATB2/zone/C/*.csv',header=True)

zone.count()



brq = spark.read.parquet('s3a://ada-prod-data/etl/data/brq/agg/agg_brq/monthly/BD/2022{04,05,06,07,08,09}/*.parquet')
brq2 = brq.select('ifa', F.explode('gps')).select('ifa', 'col.*')
brq2=brq2.select('ifa','latitude','longitude','last_seen')


from pyspark.sql.functions import date_format

brq2=brq2.withColumn("time", date_format('last_seen', 'HH'))

from pyspark.sql.types import IntegerType

brq2 = brq2.withColumn("time", brq2["time"].cast(IntegerType()))



# brq2.count()




common_ifa=zone.join(brq2,'ifa','inner')

# common_ifa=common_ifa.select("ifa")

# common_ifa=common_ifa.distinct()

# common_ifa.count()

common_ifa=common_ifa.filter( common_ifa.time >=8 )
common_ifa=common_ifa.filter( common_ifa.time <=20 )

# df_count=common_ifa.groupBy('time').agg(F.countDistinct('ifa').alias('count')).sort('count', ascending = False)
# df_count.show(50)

common_ifa=common_ifa.select('ifa','latitude','longitude')

common_ifa.show()

# common_ifa.coalesce(1).write.csv('s3a://ada-bd-emr/muntasir/BATB2/ifa_lat_long/SW/', mode='overwrite', header=True)


from pyspark.sql.functions import concat_ws,col




df=common_ifa.select(concat_ws(',',common_ifa.latitude,common_ifa.longitude)
              .alias("lat_long"),"ifa","latitude","longitude")
              

from pyspark.sql.functions import split, col
df2 = df.select(split(col("lat_long"),",").alias("lat_long_list"),"ifa","latitude","longitude")

              
from pyspark.sql import functions as F


df3=df2.groupby("ifa").agg(F.collect_set("lat_long_list"))
              


df3=df3.withColumnRenamed("collect_set(lat_long_list)","Details")



from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

def array_to_string(my_list):
    return '[' + ','.join([str(elem) for elem in my_list]) + ']'

array_to_string_udf = udf(array_to_string, StringType())



df = df3.withColumn('column_as_str', array_to_string_udf(df3["Details"]))


df=df.drop(df.Details)

df=df.withColumnRenamed("column_as_str","Detail")

df.show(50,truncate=False)






df.coalesce(1).write.csv('s3a://ada-bd-emr/muntasir/BATB2/ifa_lat_long_list/C/', mode='overwrite', header=True)






df3.write.parquet('s3a://ada-bd-emr/muntasir/BATB/gulshan_ifa_list_array/',mode='overwrite')


df=spark.read.parquet('s3a://ada-bd-emr/muntasir/BATB/gulshan_ifa_list_array/*.parquet')



