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


df=spark.read.parquet('s3a://ada-prod-data/etl/data/brq/sub/connection/monthly/BD/202111/*.parquet')



df.printSchema()


root
 |-- ifa: string (nullable = true)
 |-- req_connection: array (nullable = true)
 |    |-- element: struct (containsNull = true)
 |    |    |-- req_carrier_mcc: string (nullable = true)
 |    |    |-- req_carrier_mnc: string (nullable = true)
 |    |    |-- req_carrier_code: string (nullable = true)
 |    |    |-- req_carrier_name: string (nullable = true)
 |    |    |-- req_con_type: integer (nullable = true)
 |    |    |-- req_con_type_desc: string (nullable = true)
 |    |    |-- req_brq_count: long (nullable = true)
 |    |    |-- req_country: string (nullable = true)
 |-- mm_connection: array (nullable = true)
 |    |-- element: struct (containsNull = true)
 |    |    |-- mm_carrier_name: string (nullable = true)
 |    |    |-- mm_con_type_desc: string (nullable = true)
 |    |    |-- mm_brq_count: long (nullable = true)
 |    |    |-- mm_country: string (nullable = true)


name=df.select(F.explode('mm_connection.mm_carrier_name').alias('name'))


name_1=name.groupBy("name").agg(F.countDistinct('ifa').alias('ifa_numbers')).sort('ifa_numbers', ascending=False)



con=df.select('ifa',F.explode('mm_connection.mm_con_type_desc').alias('con'))


root
 |-- ifa: string (nullable = true)
 |-- con: string (nullable = true)


count=con.groupBy("con").agg(F.countDistinct('ifa').alias('ifa_numbers')).sort('ifa_numbers', ascending=False)


+---------+-----------+                                                         
|      con|ifa_numbers|
+---------+-----------+
| Cellular|   29293656|
|Cable/DSL|   21459357|
|Corporate|     954710|
+---------+-----------+




df2==spark.read.parquet('s3a://ada-prod-data/etl/data/brq/sub/device/monthly/BD/202111/*.parquet')



df2.select(F.explode('device.pricegrade').alias('price')).show()