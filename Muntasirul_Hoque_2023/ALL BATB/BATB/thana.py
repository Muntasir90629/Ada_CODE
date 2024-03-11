


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


brq = spark.read.parquet('s3a://ada-prod-data/etl/data/brq/agg/agg_brq/monthly/BD/2022{09}/*.parquet')

brq2 = brq.select('ifa', F.explode('gps')).select('ifa', 'col.*')


brq2=brq2.select('ifa','state','city','geohash')

brq2=brq2.filter(brq2.state == 'Dhaka')





dhaka_uh = spark.read.csv('s3a://ada-bd-emr/muntasir/BATB/UH/*.csv',header=True)

common_ifa=dhaka_uh.join(brq2,'ifa','inner')


common_ifa=common_ifa.withColumn("new_geohash", substring(col("geohash"),1,6))



common_ifa=common_ifa.drop(common_ifa.geohash)

common_ifa=common_ifa.withColumnRenamed("new_geohash","geohash")




thana_list=spark.read.csv('s3a://ada-bd-emr/muntasir/BD_LOOKUP/BD_lookup_table.csv',header=True)

thana_list=thana_list.filter(thana_list.district == 'Dhaka')






thana_ifa_uh=thana_list.join(common_ifa,'geohash','inner')






thana=thana_ifa_uh.groupBy('thana').agg(F.countDistinct('ifa').alias('count')).sort('count', ascending = False)
thana.show(50)



Dhaka


thana=thana_list.groupBy('division').agg(F.countDistinct('geohash').alias('count')).sort('count', ascending = False)
thana.show(50)


