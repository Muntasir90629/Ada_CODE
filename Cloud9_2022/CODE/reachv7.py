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


master=spark.read.csv('s3a://ada-bd-emr/muntasir/POI/bd_poi_master_v7.0.csv',header=True)

master.printSchema()

master.select('level_id','name','geohash').show()




master=master.select('level_id','name','geohash')

tax=spark.read.csv('s3a://ada-bd-emr/muntasir/POI/bd_poi_taxonomy_v7.0.csv',header=True)

tax.printSchema()

tax=tax.select('level_id','l1_name','l2_name','l3_name','l4_name')


POI_TAX=master.join(tax,"level_id","left")


print((POI_TAX.count(), len(POI_TAX.columns)))

# df=spark.read.parquet('s3a://ada-prod-data/etl/data/brq/agg/agg_brq/monthly/BD/2021{11}/*.parquet')

df=spark.read.parquet('s3a://ada-prod-data/etl/data/brq/agg/agg_brq/monthly/BD/2022{01}/*.parquet')

df.show()


df.select('ifa',F.explode('gps.geohash').alias('geohash')).show()


ifa_data=df.select('ifa',F.explode('gps.geohash').alias('geohash'))

# ifa_data.withColumn("new_geohash", substring(col("geohash"),1,6))

ifa_new=ifa_data.withColumn("new_geohash", substring(col("geohash"),1,6))

ifa_new.show()

ifa_new=ifa_new.drop(ifa_new.geohash)

ifa_new=ifa_new.withColumnRenamed("new_geohash","geohash")



REACH=POI_TAX.join(ifa_new,"geohash","left")


REACH.printSchema()


df2=REACH.select('ifa')

df2.distinct().count()


l1 =REACH.groupBy("l1_name").agg(F.countDistinct('ifa').alias('ifa_numbers')).sort('ifa_numbers', ascending=False)

l2=REACH.groupBy("l2_name").agg(F.countDistinct('ifa').alias('ifa_numbers')).sort('ifa_numbers', ascending=False)

l3 =REACH.groupBy("l3_name").agg(F.countDistinct('ifa').alias('ifa_numbers')).sort('ifa_numbers', ascending=False)

l2.show(200)

l3.show(200)





# REACH.count()


# l1=REACH.groupby("l1_name").count().show(200)


# l2=.groupby("l2_name").count().show(200)


# l3=REACH.groupby("l3_name").count().show(200)

# >>> l3.write.csv('l3_grp.csv')
# >>> l2.write.csv('l2_grp.csv')                                                  
# >>> l1.write.csv('l1_grp.csv')   