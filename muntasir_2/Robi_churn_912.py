from pyspark import SparkContext, SparkConf, HiveContext
from pyspark.sql.functions import *
from pyspark.sql.types import *
import pyspark.sql.functions as F
import pyspark.sql.types as T
import csv
import pandas as pd
import numpy as np
from pyspark.sql import Window
from pyspark.sql.functions import rank, col
import geohash2 as geohash
import pygeohash as pgh
from functools import reduce
from pyspark.sql import *


master_df = spark.read.csv('s3a://ada-dev/BD-DataScience/muntasir/DSR-912-Refresh-churn/*', header=True)

master_df.printSchema()

master_df.count()





df=spark.read.parquet('s3a://ada-prod-data/etl/data/brq/sub/connection/daily/BD/{20220901}/*.parquet')


df=df.select('ifa', F.explode('mm_connection.mm_carrier_name').alias('telco'))

robi=df.filter(df['telco']=='Robi')


robi=robi.distinct()



df_2nd=spark.read.parquet('s3a://ada-prod-data/etl/data/brq/sub/connection/monthly/BD/{202301,202302,202303,202304}/*.parquet')

df2=df_2nd.select('ifa', F.explode('mm_connection.mm_carrier_name').alias('telco2'))




final=robi.join(df2, 'ifa','left')


final=final.filter(final['telco2']!='Robi')



l1=["GrameenPhone","Banglalink","Teletalk Bangladesh"]


final=final.filter(final.telco2.isin(l1))





df_2nd=spark.read.parquet('s3a://ada-prod-data/etl/data/brq/sub/connection/monthly/BD/{202301,202302,202303,202304}/*.parquet')
df3=df_2nd.select('ifa', F.explode('mm_connection.mm_carrier_name').alias('telco3'))

df3=df3.filter(df3['telco3']=='Robi')



final2=final.join(df3, 'ifa','left')


# final2.show(300)

final2=final2.filter(final2.telco3.isNull())

final2=final2.select('ifa')
final2=final2.distinct()


final2.count()

final2=final2.withColumnRenamed("ifa","Mobile Device ID")


final2.coalesce(1).write.csv('s3a://ada-dev/BD-DataScience/muntasir/DSR-912-Refresh-churn/', mode='overwrite', header=True)







master_df = spark.read.csv('s3a://ada-dev/BD-DataScience/muntasir/ROBI_MOSQUE_842/output2/fullview-geofence/*csv', header=True)

master_df.printSchema()

master_df.count()







final2=final.select('telco2')

final2=final2.distinct().sort('telco2', ascending = True)









final2.show(200)



















f_robi=df2.filter(df2['telco2']=='Robi')

df3=df_2nd.select('ifa', F.explode('mm_connection.mm_carrier_name').alias('telco3'))
f_non_robi=df3.filter(df3['telco3']!='Robi')


ff_final=f_non_robi.join(f_robi, on='ifa', how='left')

final=robi.join(ff_final, on='ifa', how='left')

result=final.filter(final.telco3.isNotNull() & final.telco2.isNull())

result=result.select('ifa')

result=result.withColumnRenamed("ifa","Mobile Device ID")

result=result.distinct()

result.printSchema()





result.coalesce(1).write.csv('s3a://ada-dev/BD-DataScience/muntasir/DSR-912/', mode='overwrite', header=True)

result= spark.read.csv('s3a://ada-dev/BD-DataScience/muntasir/DSR-912/*.csv',header=True)
result.printSchema()
result.count()

result=result.distinct()

result.count()


result=result.withColumnRenamed("ifa","Mobile Device ID")

result.printSchema()
