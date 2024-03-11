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


df=spark.read.parquet('s3a://ada-prod-data/etl/data/brq/sub/connection/monthly/BD/202205/*.parquet')


connection = df.select('ifa', F.explode('req_connection.req_carrier_name').alias('telco'))






gp=connection.filter(connection.telco=='GrameenPhone')

gp=gp.select('ifa')

gp=gp.withColumnRenamed("ifa","mobile device id")

gp=gp.distinct()

# gp.count()


gp.coalesce(21).write.csv('s3a://ada-bd-emr/result/2022/Naagad/Persona/DSR-749/GP/', mode='overwrite', header=True)


robi=connection.filter(connection.telco=='Robi/Aktel')

robi=robi.select('ifa')

robi=robi.withColumnRenamed("ifa","mobile device id")

robi=robi.distinct()

robi.coalesce(17).write.csv('s3a://ada-bd-emr/result/2022/Naagad/Persona/DSR-749/Robi-Airtel/', mode='overwrite', header=True)

# robi.count()



bl=connection.filter(connection.telco=='Orascom/Banglalink')

bl=bl.select('ifa')

bl=bl.withColumnRenamed("ifa","mobile device id")

bl=bl.distinct()

bl.coalesce(10).write.csv('s3a://ada-bd-emr/result/2022/Naagad/Persona/DSR-749/Banglalink/', mode='overwrite', header=True)


# bl.count()


tl=connection.filter(connection.telco=='TeleTalk')

tl=tl.select('ifa')

tl=tl.withColumnRenamed("ifa","mobile device id")

tl=tl.distinct()

tl.coalesce(2).write.csv('s3a://ada-bd-emr/result/2022/Naagad/Persona/DSR-749/Teletalk/', mode='overwrite', header=True)


# tl.count()







# connection=connection.distinct()








# count=connection.groupBy('telco').agg(F.countDistinct('ifa').alias('count')).sort('count', ascending = False)