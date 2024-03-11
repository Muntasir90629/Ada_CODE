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





brq = spark.read.parquet('s3a://ada-prod-data/etl/data/brq/agg/agg_brq/monthly/BD/202211/part-00000-00864eae-e4c5-4775-8b27-2af09cf7a58e-c000.snappy.parquet')

brq.count()


# import org.apache.spark.util.SizeEstimator
# val dfSize = SizeEstimator.estimate(brq)
# println(s"Estimated size of the dataFrame weatherDF = ${dfSize/1000000} mb")






brq.write.parquet('s3a://ada-bd-emr/muntasir/BATB2/size/*.parquet', mode='overwrite')

brq.count()


# brq=brq.select('ifa')

# brq.count()
