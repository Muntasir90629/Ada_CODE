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


#load app , life stage reffrence data
master_df = spark.read.csv('s3a://ada-bd-emr/app_ref/master_df/*', header=True)

master_df.printSchema()
level_df = spark.read.csv('s3a://ada-bd-emr/app_ref/level_df/*', header=True)

level_df.printSchema()

lifestage_df = spark.read.csv('s3a://ada-bd-emr/app_ref/lifestage_df/*', header=True)
lifestage_df.printSchema()


#joining table
join_df1 = master_df.join(level_df, on='app_level_id', how='left').cache()
join_df2 = join_df1.join(lifestage_df, on='app_lifestage_id', how='left').cache()
select_columns = ['bundle', 'app_l1_name', 'app_l2_name', 'app_l3_name', 'lifestage_name']
finalapp_df = join_df2.select(*select_columns)

print("all app count")

finalapp_df.count()

finalapp_df=finalapp_df.filter(finalapp_df.app_l1_name=='eCommerce')

print("Final filter app count")

finalapp_df.count()


brq = spark.read.parquet('s3a://ada-prod-data/etl/data/brq/agg/agg_brq/monthly/BD/2022{01,02,03,04,05,06,07,08}/*.parquet')

brq2 = brq.select('ifa', F.explode('app')).select('ifa', 'col.*')


E_com_ifa=finalapp_df.join(brq2,'bundle','inner')

E_com_ifa=E_com_ifa.select('ifa')

E_com_ifa=E_com_ifa.distinct()


geo=spark.read.csv('s3a://ada-bd-emr/muntasir/Robi_795_GEO/output/fullview-geofence/*.csv', header=True)


geo=geo.select('dev_ifa')


geo=geo.withColumnRenamed("dev_ifa","ifa")

geo=geo.distinct()


# import modules
from pyspark.sql import SparkSession
import functools
 
# explicit function
def unionAll(dfs):
    return functools.reduce(lambda df1, df2: df1.union(df2.select(df1.columns)), dfs)



unioned_df = unionAll([E_com_ifa,geo])

unioned_df.printSchema()

unioned_df=unioned_df.distinct()

unioned_df.coalesce(8).write.csv('s3a://ada-bd-emr/muntasir/Robi_795_E_com_shop/', mode='overwrite', header=True)

df=spark.read.csv('s3a://ada-bd-emr/muntasir/Robi_795_E_com_shop/*.csv', header=True)


