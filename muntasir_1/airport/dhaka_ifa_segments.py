
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
from pyspark.sql import SparkSession

# spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()
# #Creates Empty RDD
# emptyRDD = spark.sparkContext.emptyRDD()
# print(emptyRDD)
# from pyspark.sql.types import StructType,StructField, StringType
# schema = StructType([StructField('ifa', StringType(), True),])
# df_em = spark.createDataFrame(emptyRDD,schema)
# df_em.printSchema()

dhaka=spark.read.csv('s3a://ada-bd-emr/muntasir/bus_two_days/count/ctg_dhaka/*.csv',header=True)


#s3://ada-bd-emr/muntasir/bus_two_days/final2/dhk_ctg/

# dhaka=dhaka.withColumn("count",col("count").cast(IntegerType))


# dhk1=dhaka.filter(dhaka.count== "1" )

#s3://ada-bd-emr/muntasir/bus_two_days/final2/ctg_dhk/

from pyspark.sql.types import IntegerType

dhaka = dhaka.withColumn("count", dhaka["count"].cast(IntegerType()))

dhaka=dhaka.withColumnRenamed("count","visit")


dhk1=dhaka.filter(dhaka.visit == 1 )


dhk1=dhk1.select('ifa')


dhk1.coalesce(1).write.csv('s3a://ada-bd-emr/muntasir/bus_two_days/final2/ctg_dhk/1/', mode='overwrite', header=True)




dhk2=dhaka.filter(dhaka.visit == 2 )


dhk2=dhk2.select('ifa')

dhk2.coalesce(1).write.csv('s3a://ada-bd-emr/muntasir/bus_two_days/final2/ctg_dhk/2/', mode='overwrite', header=True)


dhk3=dhaka.filter(dhaka.visit != 1 & dhaka.visit != 2)


dhk3=dhaka.filter(dhaka.visit >=3)


dhk3.count()

dhk3.coalesce(1).write.csv('s3a://ada-bd-emr/muntasir/bus_two_days/final2/ctg_dhk/3/', mode='overwrite', header=True)