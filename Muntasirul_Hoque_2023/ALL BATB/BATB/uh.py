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


brq = spark.read.parquet('s3a://ada-prod-data/etl/data/brq/agg/agg_brq/monthly/BD/2023{05}/*.parquet')

brq2 = brq.select('ifa', F.explode('gps')).select('ifa', 'col.*')



brq2=brq2.select('ifa','state','city')

brq2=brq2.filter(brq2.state == 'Dhaka')



affluence=spark.read.parquet('s3a://ada-prod-data/etl/data/brq/sub/affluence/monthly/BD/2022{04,05,06,07,08,09}/*.parquet')

affluence=affluence.select('ifa','final_affluence')

affluence=affluence.filter(affluence.final_affluence == 'Ultra High')





common_ifa=affluence.join(brq2,'ifa','inner')

common_ifa=common_ifa.select('ifa')

common_ifa=common_ifa.distinct()

common_ifa.count()





common_ifa.coalesce(10).write.csv('s3a://ada-bd-emr/muntasir/BATB2/UH/', mode='overwrite', header=True)

affluence=affluence.select('ifa','final_affluence','state_only')




#______________________________________________________________#


dhaka_uh = spark.read.csv('s3a://ada-bd-emr/muntasir/BATB2/UH/*.csv',header=True)

brq = spark.read.parquet('s3a://ada-prod-data/etl/data/brq/agg/agg_brq/monthly/BD/2022{09}/*.parquet')

brq2 = brq.select('ifa', F.explode('gps')).select('ifa', 'col.*')

brq2=brq2.select('ifa','state','city')



common_ifa=dhaka_uh.join(brq2,'ifa','inner')



thana=common_ifa.groupBy('city').agg(F.countDistinct('ifa').alias('count')).sort('count', ascending = False)
thana.show(50)






