
# Author        : Md.Muntasirul Hoque

# Date          : 25 March 2021

# Country       : BD

# Description   : Script to prepare analysis for built persona, for each result on each item has to be pasted on Persona Deck Template excel

# Persona Name  : UH,H,MID - INCOME



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

df=spark.read.parquet('s3a://ada-prod-data/etl/data/brq/agg/agg_brq/monthly/BD/202{112,201,202}/*.parquet')

ifa_data=df.select('ifa',F.explode('gps.geohash').alias('geohash'))

ifa_new=ifa_data.withColumn("new_geohash", substring(col("geohash"),1,7))

ifa_new.show()

ifa_new=ifa_new.drop(ifa_new.geohash)

ifa_new=ifa_new.withColumnRenamed("new_geohash","geohash")

#Adding affluence according from -( affluence dataset) present dataframe 


affluence=spark.read.parquet('s3a://ada-prod-data/etl/data/brq/sub/affluence/monthly/BD/202{112,201,202}/*.parquet')



#taking important columns for affluence

affluence=affluence.select('ifa','final_affluence')


affluence=affluence.withColumnRenamed("final_affluence","affluence")


# l1 =affluence.groupBy("affluence").agg(F.countDistinct('ifa').alias('ifa_numbers')).sort('ifa_numbers', ascending=False)

# l1.show()

affluence_final=affluence.filter(affluence.affluence =='Ultra High')


# affluence_final.show()


# affluence_final=affluence.filter(affluence.affluence!='Low')

# affluence_final=affluence_final.filter(affluence.affluence!='Mid')


df_uh_h=affluence_final.join(ifa_new,"ifa","left")



df_uh_h=df_uh_h.filter(df_uh_h.affluence !='null') 
df_uh_h=df_uh_h.filter(df_uh_h.geohash !='null')


#final dataframe 

# df_uh_h.show()

# master_poi=spark.read.csv('s3a://ada-bd-emr/muntasir/POI/bd_poi_master_geo9_v7.0.csv',header=True)


# master_poi=master_poi.select('level_id','name','geohash9')

# master_poi=master_poi.withColumn("geohash", substring(col("geohash9"),1,8))

# master_poi=master_poi.drop(master_poi.geohash9)

# master_poi.show()



# jewellers_poi=master_poi.filter(master_poi.level_id.startswith('RET_08_01_'))

# pharma_poi=master_poi.filter(master_poi.level_id.startswith('RET_11_01_'))

# automobile_poi=master_poi.filter(master_poi.level_id.startswith('AUT_02_01_'))

# poshclub_poi=master_poi.filter((master_poi.level_id.startswith('ENT_05_01_001')) & (master_poi.name.endswith('club')))

# bank_poi=master_poi.filter(master_poi.level_id.startswith('FIN_01_01_'))




# df_uh__h_jewellery=jewellers_poi.join(df_uh_h,'geohash','left')

# df2=df_uh__h_jewellery.select('ifa')

# df2.distinct().count()



# df_uh__h_pharma=pharma_poi.join(df_uh_h,'geohash','left')

# df2=df_uh__h_pharma.select('ifa')

# df2.distinct().count()



# df_uh__h_auto=df_uh_h.join(automobile_poi,'geohash','left')

# df2=df_uh__h_auto.select('ifa')

# df2.distinct().count()


# df_uh_h_club=poshclub_poi.join(df_uh_h,'geohash','left')

# df2=df_uh_h_club.select('ifa')

# df2.distinct().count()


# df_uh_h_bank=bank_poi.join(df_uh_h,'geohash','left')


# df2=df_uh_h_bank.select('ifa')

# df2.distinct().count()


garments=spark.read.csv('s3a://ada-bd-emr/muntasir/location/cement_factories_real_geo.csv',header=True)

garments=garments.select('name','geohash9')

garments=garments.withColumn("geohash", substring(col("geohash9"),1,7))

garments=garments.drop(garments.geohash9)

garments.show()


garments_cnt=garments.join(df_uh_h,'geohash','left')

df2=garments_cnt.select('ifa')

df2.distinct().count()
