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

df=spark.read.parquet('s3a://ada-prod-data/etl/data/brq/agg/agg_brq/monthly/BD/2022{02}/*.parquet')


df.show()


df.printSchema()





df2=df.select('ifa','device.device_vendor','device.device_model','device.major_os',F.explode('gps.geohash').alias('geohash'))


df2=df2.withColumnRenamed("device.device_vendor","Vendor")


df2=df2.withColumnRenamed("device.device_model","Model")


df2=df2.withColumnRenamed("device.major_os","OS")


df2=df2.withColumn("geohash_new", substring(col("geohash"),1,6))


df2=df2.drop('geohash')


df2=df2.withColumnRenamed("geohash_new","geohash")




df2.printSchema()

pabna=spark.read.csv('s3a://ada-bd-emr/muntasir/place/pabna.csv',header=True)


gulshan=spark.read.csv('s3a://ada-bd-emr/muntasir/place/gulshan.csv',header=True)



pabna_ifa=pabna.join(df2,"geohash","left")



pabna_ifa.printSchema()





vendor=pabna_ifa.groupBy("Vendor").agg(F.countDistinct('ifa').alias('ifa_numbers')).sort('ifa_numbers', ascending=False)

model=pabna_ifa.groupBy("Model").agg(F.countDistinct('ifa').alias('ifa_numbers')).sort('ifa_numbers', ascending=False)

os=pabna_ifa.groupBy("OS").agg(F.countDistinct('ifa').alias('ifa_numbers')).sort('ifa_numbers', ascending=False)


vendor.show(20)

+--------+-----------+                                                          
|  Vendor|ifa_numbers|
+--------+-----------+
| Samsung|      72516|
|  Xiaomi|      45800|
|    Vivo|      39140|
|    null|      36551|
|  Realme|      26946|
|    OPPO|      26845|
|Symphony|      11822|
|   Tecno|      11348|
|    itel|      11130|
|  Huawei|      10415|
|  Walton|       8841|
| Generic|       5948|
| Infinix|       5653|
|   Nokia|       2340|
| OnePlus|       2240|
|   Apple|       2036|
|    Lava|       1952|
|  Lenovo|        755|
|Motorola|        510|
|      LG|        297|
+--------+-----------+
only showing top 20 rows



model.show(20)

>>> model.show(20)
+------------+-----------+                                                      
|       Model|ifa_numbers|
+------------+-----------+
|        null|      36551|
|        1906|       5947|
|       V2043|       5611|
|       V2111|       3343|
|     RMX3195|       3062|
|Redmi Note 8|       3009|
|    SM-M215F|       2922|
|       V2026|       2888|
|     CPH2185|       2865|
|     RMX3201|       2861|
|     CPH2083|       2732|
|       L6502|       2708|
|       V2027|       2656|
|       V2102|       2511|
|  M2003J15SC|       2458|
|       X688B|       2231|
|     CPH2269|       2228|
|    SM-M022G|       2207|
|        1820|       2109|
|     RMX2030|       2080|
+------------+-----------+
only showing top 20 rows



os.show(20)


>>> os.show(20)

+------------+-----------+                                                      
|          OS|ifa_numbers|
+------------+-----------+
|Android 11.0|     112031|
|Android 10.0|      72908|
| Android 9.0|      37835|
|        null|      36551|
| Android 8.1|      25455|
| Android 6.0|      13519|
| Android 5.1|       8251|
| Android 7.0|       5189|
| Android 7.1|       4595|
| Android 8.0|       3113|
|Android 12.0|       1899|
| Android 4.4|       1859|
| Android 5.0|        746|
|    iOS 12.0|        395|
|    iOS 15.2|        349|
|    iOS 15.3|        270|
|    iOS 15.1|        176|
|    iOS 14.8|        154|
| Android 4.2|        130|
|    iOS 14.7|         95|
+------------+-----------+
only showing top 20 rows



gulshan_ifa=gulshan.join(df2,"geohash","left")


vendor=gulshan_ifa.groupBy("Vendor").agg(F.countDistinct('ifa').alias('ifa_numbers')).sort('ifa_numbers', ascending=False)

model=gulshan_ifa.groupBy("Model").agg(F.countDistinct('ifa').alias('ifa_numbers')).sort('ifa_numbers', ascending=False)

os=gulshan_ifa.groupBy("OS").agg(F.countDistinct('ifa').alias('ifa_numbers')).sort('ifa_numbers', ascending=False)


vendor.show(20)


model.show(20)


os.show(20)
