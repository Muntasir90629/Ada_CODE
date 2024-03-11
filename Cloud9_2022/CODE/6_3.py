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


p1=spark.read.csv('s3a://ada-bd-emr/muntasir/POI-V6-RECH-COUNT/output/p1/fullview-geofence/*.csv',header=True)

p1.printSchema()

p2=spark.read.csv('s3a://ada-bd-emr/muntasir/POI-V6-RECH-COUNT/output/p2/fullview-geofence/*.csv',header=True)

p2.printSchema()

p3=spark.read.csv('s3a://ada-bd-emr/muntasir/POI-V6-RECH-COUNT/output/p3/fullview-geofence/*.csv',header=True)

p3.printSchema()

p4=spark.read.csv('s3a://ada-bd-emr/muntasir/POI-V6-RECH-COUNT/output/p4/fullview-geofence/*.csv',header=True)

p4.printSchema()



from functools import reduce

from pyspark.sql import DataFrame



dfs = [p1,p2,p3,p4]

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

master=spark.read.csv('s3a://ada-bd-emr/muntasir/POI_6/bd_poi_master_v6.0.csv',header=True)

master.select('level_id','name','geohash').show()




master=master.select('level_id','name','geohash')

master.printSchema()


master=master.withColumn("level_id_new", substring(col("level_id"),1,9))


tax=spark.read.csv('s3a://ada-bd-emr/muntasir/POI_6/bd_poi_taxonomy_v6.0.csv',header=True)



tax=tax.select('level_id','l1_name','l2_name','l3_name','l4_name')

tax=tax.withColumn("level_id_new", substring(col("level_id"),1,9))




master_ifa=df.join(master,"name","left")




REACH=master_ifa.join(tax,"level_id_new","left")


# df2=REACH.select('ifa')

# df2.distinct().count()


l1 =REACH.groupBy("l1_name").agg(F.countDistinct('ifa').alias('ifa_numbers')).sort('ifa_numbers', ascending=False)

l2=REACH.groupBy("l2_name").agg(F.countDistinct('ifa').alias('ifa_numbers')).sort('ifa_numbers', ascending=False)

l3 =REACH.groupBy("l3_name").agg(F.countDistinct('ifa').alias('ifa_numbers')).sort('ifa_numbers', ascending=False)


l1.show(200)

l2.show(200)

l3.show(200)





