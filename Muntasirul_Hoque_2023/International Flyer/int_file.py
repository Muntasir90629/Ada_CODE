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


df1=spark.read.csv('s3a://ada-bd-emr/muntasir/International_Flyer/output/fullview-geofence/*.csv',header=True)

df1.count()


df2=spark.read.csv('s3a://ada-bd-emr/muntasir/International_Flyer/output2/fullview-geofence/*.csv',header=True)

df2.count()


df3=spark.read.csv('s3a://ada-bd-emr/muntasir/International_Flyer/output3/fullview-geofence/*.csv',header=True)
df3.count()

df4=spark.read.csv('s3a://ada-bd-emr/muntasir/International_Flyer/output4/fullview-geofence/*.csv',header=True)
df4.count()




from functools import reduce 
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
 
# explicit functions
def unionAll(*dfs):
    return reduce(DataFrame.unionAll, dfs)
 



merge=unionAll(*[df1, df2, df3,df4])

merge.count()

merge=merge.withColumnRenamed("dev_ifa","ifa")


from pyspark.sql.functions import date_format

merge=merge.withColumn("month", date_format('dev_datetime_local', 'MM'))

merge.show()

merge=merge.withColumn("year", date_format('dev_datetime_local', 'yyyy'))

merge.show()



find_ifa=merge.select("ifa","year","month")

find_ifa.count()

find_ifa=find_ifa.distinct()


find_ifa.count()


df_em=find_ifa.groupBy("ifa").count()


df_em=df_em.withColumnRenamed("count","freq")


df_em.show()

df_em.printSchema()


Frequen_International_Flyer=df_em.filter(df_em.freq >=2)
Frequen_International_Flyer=Frequen_International_Flyer.select('ifa')
Frequen_International_Flyer=Frequen_International_Flyer.distinct()
Frequen_International_Flyer.count()

Frequen_International_Flyer=Frequen_International_Flyer.withColumnRenamed("ifa","Mobile Device ID")

Frequen_International_Flyer.show()

Frequen_International_Flyer.coalesce(1).write.csv('s3a://ada-bd-emr/muntasir/International_Flyer/freq_int_flyer/', mode='overwrite', header=True)

Casual_International_Flyer=df_em.filter(df_em.freq <2)
Casual_International_Flyer=Casual_International_Flyer.select('ifa')
Casual_International_Flyer=Casual_International_Flyer.distinct()
Casual_International_Flyer.count()

Casual_International_Flyer=Casual_International_Flyer.withColumnRenamed("ifa","Mobile Device ID")


Casual_International_Flyer.coalesce(1).write.csv('s3a://ada-bd-emr/muntasir/International_Flyer/casual_flyer/', mode='overwrite', header=True)


Casual_International_Flyer.show()


# As per previous discussion, need two segments based on
# * Casual International Flyer
# * Frequent International Flyer












