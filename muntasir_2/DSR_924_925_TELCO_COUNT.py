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


df=spark.read.parquet('s3a://ada-prod-data/etl/data/brq/sub/connection/monthly/BD/{202302,202303,202304}/*.parquet')

connection = df.select('ifa', F.explode('req_connection.req_carrier_name').alias('telco'))

# count=connection.groupBy('telco').agg(F.countDistinct('ifa').alias('count')).sort('count', ascending = False)

# count.show(50)


gp=connection.filter(connection.telco=='GrameenPhone')

gp=gp.select('ifa')

gp=gp.withColumnRenamed("ifa","mobile device id")

gp=gp.distinct()

# gp.count()

robi=connection.filter(connection.telco=='Airtel')

robi=robi.select('ifa')

robi=robi.withColumnRenamed("ifa","mobile device id")

robi=robi.distinct()

# robi.count()

bl=connection.filter(connection.telco=='Banglalink')

bl=bl.select('ifa')

bl=bl.withColumnRenamed("ifa","mobile device id")

bl=bl.distinct()

# bl.count()

tl=connection.filter(connection.telco=='TeleTalk')

tl=tl.select('ifa')

tl=tl.withColumnRenamed("ifa","mobile device id")

tl=tl.distinct()

# tl.count()


master_df = spark.read.csv('s3://ada-dev/BD-DataScience/muntasir/Nagad_Telco_DSR-925/DSR-925/Teletalk/*', header=True)

master_df.count()




gp.coalesce(1).write.csv('s3a://ada-dev/BD-DataScience/muntasir/Nagad_Telco_DSR-925/DSR-925/GP/', mode='overwrite', header=True)

robi.coalesce(1).write.csv('s3a://ada-dev/BD-DataScience/muntasir/Nagad_Telco_DSR-925/DSR-925/ROBI/', mode='overwrite', header=True)

bl.coalesce(1).write.csv('s3a://ada-dev/BD-DataScience/muntasir/Nagad_Telco_DSR-925/DSR-925/Banglalink/', mode='overwrite', header=True)

tl.coalesce(1).write.csv('s3://ada-dev/BD-DataScience/muntasir/Nagad_Telco_DSR-925/DSR-925/Teletalk/', mode='overwrite', header=True)



# connection=connection.distinct()








# count=connection.groupBy('telco').agg(F.countDistinct('ifa').alias('count')).sort('count', ascending = False)