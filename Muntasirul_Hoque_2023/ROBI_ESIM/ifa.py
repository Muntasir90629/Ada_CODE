pkg_list=com.databricks:spark-avro_2.11:4.0.0,org.apache.hadoop:hadoop-aws:2.7.1
pyspark --packages $pkg_list --driver-memory 10G --driver-cores 5 --num-executors 29 --executor-memory 10G --executor-cores 5 --conf spark.driver.memoryOverhead=512 --conf spark.debug.maxToStringFields=100 --conf spark.driver.maxResultSize=0 --conf spark.yarn.maxAppAttempts=1 --conf s7park.ui.port=10045




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


df=spark.read.parquet('s3a://ada-prod-data/etl/data/brq/agg/agg_brq/monthly/BD/{202101,202102,202103,202104,202105,202106,202107,202108,202109,202110,202111,202112,202201,202202,202203,202204,202205,202206,202207,202208,202209,202210,202211,202212}/*.parquet')

df=df.select('ifa','device.device_name','device.device_vendor','device.device_year_of_release','device.major_os','device.device_manufacturer','device.device_model')


iphone=df.filter(df.device_vendor== 'Apple')

iphone=iphone.withColumn("year", substring(col("device_year_of_release"),0,4))

iphone=iphone.filter(~F.col('major_os').startswith('iPadOS'))
iphone=iphone.filter(~F.col('major_os').startswith('Android'))
iphone=iphone.filter(~F.col('major_os').startswith('TV'))


iphone=iphone.withColumn("version", substring(col("major_os"),5,4))

from pyspark.sql.types import IntegerType

iphone =iphone.withColumn("version", iphone["version"].cast(FloatType()))

iphone=iphone.filter(iphone.version >= 14.0)
iphone=iphone.select('ifa')
iphone=iphone.distinct()
# iphone.count()


li=["iPhone 14",	"iPhone 14 Plus",	"iPhone 14 Pro Max",	"iPhone 14 Pro",	"iPhone 13",	"iPhone 13 Pro",	"iPhone 13 Pro Max",	"iPhone 13 Mini",	"iPhone 12",	"iPhone 12 Pro",	"iPhone 12 Pro Max",	"iPhone 12 Mini",	"iPhone SE",	"iPhone 11",	"iPhone 11 Pro",	"iPhone 11 Pro Max",	"iPhone XS",	"iPhone XS Max",	"iPhone XR",	"Google Pixel 7 Pro",	"Google Pixel 7",	"Google Pixel 6 Pro",	"Google Pixel 6",	"Google Pixel 5a 5G",	"Google Pixel 5",	"Google Pixel 4a",	"Google Pixel 4",	"Google Pixel 3",	"Google Pixel 3XL",	"Google Pixel 2",	"Samsung Galaxy S22 5G",	"Samsung Galaxy S22 Ultra 5G",	"Samsung Galaxy S22",	"Samsung Fold LTE model",	"Samsung Z Flip 4",	"Samsung Z Fold 4",	"Samsung Galaxy Z Fold 3 5G",	"Samsung Galaxy Z Flip 5G",	"Samsung Galaxy Z Flip",	"Samsung Galaxy Z Fold2 5G",	"Samsung Galaxy Fold",	"Samsung Galaxy S21+ 5G",	"Samsung Galaxy S21 Ultra 5G",	"Samsung Galaxy Note 20 Ultra",	"Samsung Galaxy Note 20 Ultra 5G",	"Samsung Galaxy Note 20 FE 5G",	"Samsung Galaxy Note 20 FE",	"Samsung Galaxy S20",	"Samsung Galaxy S20+",	"Samsung Galaxy S20 Ultra",	"Vivo X80",	"Huawei P40",	"Huawei P40 Pro",	"Huawei Mate 40",	"Oppo Find X3",	"Oppo Find X3 Pro",	"Oppo Find X5",	"Oppo Find X5 Pro"]

esim=df.filter(df.device_name.isin(li))

esim=esim.select('ifa')

esim=esim.distinct()

# esim.count()


common_ifa=iphone.join(esim,'ifa','outer')

common_ifa=common_ifa.distinct()

# common_ifa.count()



df2=spark.read.parquet('s3a://ada-prod-data/etl/data/brq/agg/agg_brq/monthly/BD/{202101,202102,202103,202104,202105,202106,202107,202108,202109,202110,202111,202112,202201,202202,202203,202204,202205,202206,202207,202208,202209,202210,202211,202212}/*.parquet')


con=df2.select('ifa', F.explode('connection')).select('ifa', 'col.*')

con=con.select('ifa','mm_carrier_name')

l=['GrameenPhone','Robi','Banglalink','Teletalk Bangladesh']

con=con.filter(con.mm_carrier_name.isin(l))





e_sim2=common_ifa.join(con,"ifa","left")

e_sim2=e_sim2.dropna()




e_sim2=e_sim2.filter(e_sim2.mm_carrier_name == 'Banglalink')

e_sim2=e_sim2.select("ifa")

e_sim2=e_sim2.distinct()


e_sim2.count()


e_sim2=e_sim2.withColumnRenamed("ifa","Mobile Device ID")

e_sim2.printSchema()


e_sim2.coalesce(1).write.csv('s3a://ada-bd-emr/muntasir/E_SIM/Banglalink/', mode='overwrite', header=True)


s3://ada-bd-emr/muntasir/E_SIM/ROBI/
s3://ada-bd-emr/muntasir/E_SIM/GP/
s3://ada-bd-emr/muntasir/E_SIM/BL/





















freq_beh=e_sim2.groupBy('mm_carrier_name').agg(F.countDistinct('ifa').alias('freq')).sort('freq', ascending = False)

freq_beh.show(10,truncate=False)


li=["Robi/Aktel","Airtel"]


e_sim2=e_sim2.filter(e_sim2.telco.isin(li))

e_sim2.show()


e_sim2=e_sim2.select("ifa")

e_sim2.count()














freq_beh=e_sim.groupBy('telco').agg(F.countDistinct('ifa').alias('freq')).sort('freq', ascending = False)

freq_beh.show(10,truncate=False)

