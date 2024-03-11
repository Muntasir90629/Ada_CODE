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



p1=spark.read.csv('s3a://ada-bd-emr/muntasir/POI-REACH-COUNT-MUNTASIR/output/p1/fullview-geofence/*.csv',header=True)

p1.printSchema()


p2=spark.read.csv('s3a://ada-bd-emr/muntasir/POI-REACH-COUNT-MUNTASIR/output/p2/fullview-geofence/*.csv',header=True)

p2.printSchema()


p3=spark.read.csv('s3a://ada-bd-emr/muntasir/POI-REACH-COUNT-MUNTASIR/output/p3/fullview-geofence/*.csv',header=True)


p3.printSchema()


p4=spark.read.csv('s3a://ada-bd-emr/muntasir/POI-REACH-COUNT-MUNTASIR/output/p4/fullview-geofence/*.csv',header=True)

p4.printSchema()

p5=spark.read.csv('s3a://ada-bd-emr/muntasir/POI-REACH-COUNT-MUNTASIR/output/p5/fullview-geofence/*.csv',header=True)


p5.printSchema()

p6=spark.read.csv('s3a://ada-bd-emr/muntasir/POI-REACH-COUNT-MUNTASIR/output/p6/fullview-geofence/*.csv',header=True)

p6.printSchema()

p7=spark.read.csv('s3a://ada-bd-emr/muntasir/POI-REACH-COUNT-MUNTASIR/output/p7/fullview-geofence/*.csv',header=True)


p7.printSchema()

p8=spark.read.csv('s3a://ada-bd-emr/muntasir/POI-REACH-COUNT-MUNTASIR/output/p8/fullview-geofence/*.csv',header=True)


p8.printSchema()



p9=spark.read.csv('s3a://ada-bd-emr/muntasir/POI-REACH-COUNT-MUNTASIR/output/P9_1/fullview-geofence/*.csv',header=True)


p9.printSchema()

p9_1=spark.read.csv('s3a://ada-bd-emr/muntasir/POI-REACH-COUNT-MUNTASIR/output/P9_2/fullview-geofence/*.csv',header=True)

p9_1.printSchema()


p10=spark.read.csv('s3a://ada-bd-emr/muntasir/POI-REACH-COUNT-MUNTASIR/output/p10/fullview-geofence/*.csv',header=True)


p10.printSchema()


from functools import reduce

from pyspark.sql import DataFrame



dfs = [p1,p2,p3,p4,p5,p6,p7,p8,p9,p9_1,p10]

# create merged dataframe
df_complete = reduce(DataFrame.unionAll, dfs)



df=df_complete.select('dev_ifa','poi_name')



df.printSchema()


df=df.withColumnRenamed("dev_ifa","ifa")

df.printSchema()







df=df.withColumnRenamed("poi_name","name")


df.printSchema()


df2=df.select('ifa')

df2.distinct().count()


master=spark.read.csv('s3a://ada-bd-emr/muntasir/POI/bd_poi_master_v7.0.csv',header=True)

master.printSchema()

master.select('level_id','name','geohash').show()




master=master.select('level_id','name')


master_ifa=df.join(master,"name","left")


tax=spark.read.csv('s3a://ada-bd-emr/muntasir/POI/bd_poi_taxonomy_v7.0.csv',header=True)

tax.printSchema()

tax=tax.select('level_id','l1_name','l2_name','l3_name')





REACH=master_ifa.join(tax,"level_id","left")


# df2=REACH.select('ifa')

# df2.distinct().count()


l1 =REACH.groupBy("l1_name").agg(F.countDistinct('ifa').alias('ifa_numbers')).sort('ifa_numbers', ascending=False)

l2=REACH.groupBy("l2_name").agg(F.countDistinct('ifa').alias('ifa_numbers')).sort('ifa_numbers', ascending=False)

l3 =REACH.groupBy("l3_name").agg(F.countDistinct('ifa').alias('ifa_numbers')).sort('ifa_numbers', ascending=False)


l1.show(200)

l2.show(200)

l3.show(200)
