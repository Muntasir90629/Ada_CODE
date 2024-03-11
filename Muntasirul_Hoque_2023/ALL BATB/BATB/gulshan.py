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


dhaka_uh = spark.read.csv('s3a://ada-bd-emr/muntasir/BATB/UH/*.csv',header=True)



brq = spark.read.parquet('s3a://ada-prod-data/etl/data/brq/sub/age/monthly/NP/2022{01}/*.parquet')




brq2 = brq.select('ifa', F.explode('gps')).select('ifa', 'col.*')
brq2=brq2.select('ifa','state','city','geohash','latitude','longitude')
brq2=brq2.filter(brq2.state == 'Dhaka')



dhaka_uh = spark.read.csv('s3a://ada-bd-emr/muntasir/BATB/UH/*.csv',header=True)
common_ifa=dhaka_uh.join(brq2,'ifa','inner')

# common_ifa=common_ifa.select('ifa')
# common_ifa=common_ifa.distinct()
# common_ifa.count()

common_ifa=common_ifa.withColumn("new_geohash", substring(col("geohash"),1,6))
common_ifa=common_ifa.drop(common_ifa.geohash)
common_ifa=common_ifa.withColumnRenamed("new_geohash","geohash")




thana_list=spark.read.csv('s3a://ada-bd-emr/muntasir/BD_LOOKUP/BD_lookup_table.csv',header=True)
thana_list=thana_list.filter(thana_list.district == 'Dhaka')
thana_ifa_uh=thana_list.join(common_ifa,'geohash','inner')

thana_ifa_uh=thana_ifa_uh.filter(thana_ifa_uh.thana == 'Gulshan')
thana_ifa_uh=thana_ifa_uh.select('ifa','thana','latitude','longitude')
thana_df=thana_ifa_uh.select('ifa','latitude','longitude')
thana_df=thana_df.distinct()


thana_df.printSchema()




from pyspark.sql.functions import concat_ws,col




df=thana_df.select(concat_ws(',',thana_df.latitude,thana_df.longitude)
              .alias("lat_long"),"ifa","latitude","longitude")
              

from pyspark.sql.functions import split, col
df2 = df.select(split(col("lat_long"),",").alias("lat_long_list"),"ifa","latitude","longitude")

              
from pyspark.sql import functions as F


df3=df2.groupby("ifa").agg(F.collect_set("lat_long_list"))
              


df3=df3.withColumnRenamed("collect_set(lat_long_list)","Details")


# from pyspark.sql.functions import udf
# from pyspark.sql.types import StringType
# def array_to_string(my_list):
    
#     return '[' + ','.join([str(elem) for elem in my_list]) + ']'

# array_to_string_udf=udf(array_to_string, StringType())

# df = df3.withColumn('column_as_str', array_to_string_udf(df3["Details"]))


# from pyspark.sql.functions import col, concat_ws
# df2 = df3.withColumn("Details",
#   concat_ws(",",col("Details")))
# df2.printSchema()
# df2.show(truncate=False)

from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

def array_to_string(my_list):
    return '[' + ','.join([str(elem) for elem in my_list]) + ']'

array_to_string_udf = udf(array_to_string, StringType())



df = df3.withColumn('column_as_str', array_to_string_udf(df3["Details"]))


df=df.drop(df.Details)

df=df.withColumnRenamed("column_as_str","Detail")




df.coalesce(1).write.csv('s3a://ada-bd-emr/muntasir/BATB/gulshan_ifa_csv/', mode='overwrite', header=True)






df3.write.parquet('s3a://ada-bd-emr/muntasir/BATB/gulshan_ifa_list_array/',mode='overwrite')


df=spark.read.parquet('s3a://ada-bd-emr/muntasir/BATB/gulshan_ifa_list_array/*.parquet')




thana_ifa_uh_gul.count()
thana_ifa_uh_gul=thana_ifa_uh_gul.distinct()
thana_ifa_uh_gul.count()


thana_ifa_uh_gul.coalesce(1).write.csv('s3a://ada-bd-emr/muntasir/BATB/gulshan_lat_lon/', mode='overwrite', header=True)



# common_ifa=common_ifa.select('ifa')
# common_ifa=common_ifa.distinct()
# common_ifa.count()


