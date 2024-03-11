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
from functools import reduce
from pyspark.sql import *
from pyspark.sql import DataFrame


df=spark.read.parquet('s3a://ada-prod-data/etl/data/brq/agg/agg_brq/monthly/BD/2022{03,04,05}/*.parquet')

df.printSchema()

gps_brq=df.select('ifa',explode('gps.city').alias('city'), 'brq_count')
# gps_brq.select('city').distinct().show()

dhaka_ifa=gps_brq.filter(gps_brq.city=='Dhaka')
chittagong_ifa=gps_brq.filter(gps_brq.city=='Chittagong')
# common_ifa=day1_dhk.join(day2_ctg,'ifa','inner')

dfs = [dhaka_ifa,chittagong_ifa]
    
    # create merged dataframe
    
df_complete = reduce(DataFrame.unionAll, dfs)

# df_complete.show()

# chittagong_ifa=df_complete.filter(df_complete.city=='Chittagong').show(10,False)
# df_complete.count()
# df_complete=df_complete.select('ifa')
# df_complete=df_complete.distinct()
# df_complete.count()
df_complete.coalesce(1).write.parquet('s3a://ada-bd-emr/omi/safi/',mode='overwrite',header=True)