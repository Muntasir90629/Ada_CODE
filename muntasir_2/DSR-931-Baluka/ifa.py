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
from pyspark.sql import SparkSession

from pyspark.sql.functions import date_format

df=spark.read.csv('s3a://ada-dev/BD-DataScience/muntasir/Baluka_DSR_931/full_geo/*.csv',header=True)

df.count()



df=df.withColumnRenamed("dev_ifa","ifa")
# df=df.withColumnRenamed("dev_datetime_local","date")
df=df.withColumnRenamed("dev_carrier","carrier")
df=df.withColumnRenamed("dev_lat","lat")
df=df.withColumnRenamed("dev_lon","lon")
df=df.withColumnRenamed("poi_name","name")

df=df.withColumn("date", substring(col("dev_datetime_local"),1,10))

df.show()


# The Westin Dhaka



df1=df.filter(df.name == 'The Westin Dhaka')


li=["2022-12-09"]

df1=df1.filter(df1.date.isin(li))

df1.show()

df1.count()




# Kurmitola Golf Club


df2=df.filter(df.name == 'Kurmitola Golf Club')


li=["2022-11-13"]

df2=df2.filter(df2.date.isin(li))

df2.show()

df2.count()


# BICC




df3=df.filter(df.name == 'Bangabandhu International Conference Center')

df3.show()


li=["2022-11-14","2023-03-11","2023-03-12","2023-03-13"]

df3=df3.filter(df3.date.isin(li))

df3.show()

df3.count()





# Radisson Blu Hotel DHAKA



df4=df.filter(df.name == 'Radisson Blu Hotel DHAKA')

df4.show()


li=["2022-11-14","2022-11-15","2022-11-23","2022-11-24","2022-12-03","2022-12-07"]

df4=df4.filter(df4.date.isin(li))

df4.show()

df4.count()





# ISD Campus


df5=df.filter(df.name == 'ISD Campus')

df5.show()


li=["2022-10-25","2022-09-28","2022-08-30","2022-01-31","2022-06-19"]

df5=df5.filter(df5.date.isin(li))

df5.show()

df5.count()



# AISD Campus


df6=df.filter(df.name == 'AISD')

df6.show()


li=["2023-02-22","2023-03-05","2023-03-06","2023-03-07","2023-03-08","2023-03-09","2022-12-05","2022-12-01","2022-10-23","2022-09-10"]

df6=df6.filter(df6.date.isin(li))

df6.show()

df6.count()



# Grace International School



df15=df.filter(df.name == 'Grace International School Bangladesh')

df15.show()



li=["2022-11-28","2022-09-22","2022-08-22","2022-08-23","2022-08-24"]
    
df15=df15.filter(df15.date.isin(li))

df15.show()

df15.count()




#  Anita and Samson H Chowdhury Performing Arts Center AISD Campus


df7=df.filter(df.name == 'Anita and Samson H Chowdhury Performing Arts Center AISD Campus')

df7.show()

li=["2022-12-06",]
    
df7=df7.filter(df7.date.isin(li))

df7.show()

df7.count()


# Grand Palace Sylhet




df8=df.filter(df.name == 'Grand Palace Sylhet')

df8.show()

li=["2023-03-20","2022-09-22"]
    
df8=df8.filter(df8.date.isin(li))

df8.show()

df8.count()


# Silchar Police Parade Ground



# df9=df.filter(df.name == 'Silchar Police Parade Ground')

# df9.show()

# li=["2022-12-02","2022-12-03"]
    
# df9=df9.filter(df9.date.isin(li))

# df9.show()

# df9.count()


# GEC Convention Centre


df10=df.filter(df.name == 'GEC Convention Centre')

df10.show()

li=["2022-11-17","2022-11-18","2022-11-19"]
    
df10=df10.filter(df10.date.isin(li))

df10.show()

df10.count()


#  Radisson Blu Chattogram Bay View



df11=df.filter(df.name == 'Radisson Blu Chattogram Bay View')

df11.show()

li=["2022-04-14","2022-04-15","2022-04-16"]
    
df11=df11.filter(df11.date.isin(li))

df11.show()

df11.count()


# BPC

df12=df.filter(df.name == 'BPC')

df12.show()

li=["2022-12-04"]
    
df12=df12.filter(df12.date.isin(li))

df12.show()

df12.count()


# FBCCI 



df13=df.filter(df.name == 'FBCCI')

df13.show()

li=["2022-12-31"]
    
df13=df13.filter(df13.date.isin(li))

df13.show()

df13.count()



# Bangladesh Marine Academy 



# df13=df.filter(df.name == 'Bangladesh Marine Academy')

# df13.show()

# li=["2022-12-04"]
    
# df13=df13.filter(df13.date.isin(li))

# df13.show()

# df13.count()



# Hall 2  ICCB
# Hall 1  ICCB
# Hall 3  ICCB




li=["Hall 2  ICCB","Hall 1  ICCB","Hall 3  ICCB"]
df14=df.filter(df.name.isin(li))

df14.show()

li=["2022-11-16","2022-11-15"]
    
df14=df14.filter(df14.date.isin(li))

df14.show()

df14.count()




from functools import reduce
from pyspark.sql import DataFrame



dfs = [df1,df2,df3,df4,df5,df7,df8,df10,df11,df12,df14]

# create merged dataframe
df_complete = reduce(DataFrame.unionAll, dfs)

df_complete=df_complete.select('ifa')

df_complete =df_complete.distinct()


df_complete.count()


df_complete.printSchema()


df2=spark.read.csv('s3a://ada-dev/BD-DataScience/muntasir/Baluka_DSR_931/total_ifa/',header=True)


df2.count()

dfs = [df_complete,df2]


df_complete = reduce(DataFrame.unionAll, dfs)

df_complete=df_complete.select('ifa')

df_complete =df_complete.distinct()


df_complete.count()


df_complete=df_complete.withColumnRenamed("ifa","Mobile Device ID")

df_complete.printSchema()


df_complete.coalesce(1).write.csv('s3a://ada-dev/BD-DataScience/muntasir/Baluka_DSR_931/final_ifa/final1/', mode='overwrite', header=True)


df=spark.read.csv('s3a://ada-dev/BD-DataScience/muntasir/Baluka_DSR_931/final_ifa/final1/*.csv',header=True)


s3://ada-dev/BD-DataScience/muntasir/Baluka_DSR_931/final_ifa/final1/



############################################################

aff = spark.read.parquet('s3a://ada-business-insights/prod/affluence/affluence_result/BD/2023{05}/*.parquet')


aff=aff.filter(aff.affluence_level == 'high')

aff=aff.select('ifa')

aff=aff.distinct()

aff.count()



# affluence=spark.read.parquet('s3a://ada-prod-data/etl/data/brq/sub/affluence/monthly/BD/2022{01,02,03}/*.parquet')

# affluence=affluence.select('ifa','final_affluence')

# affluence=affluence.filter(affluence.final_affluence == 'Ultra High')





brq = spark.read.parquet('s3a://ada-prod-data/etl/data/brq/agg/agg_brq/daily/BD/202305{30}/*.parquet')




brq2 = brq.select('ifa', F.explode('gps')).select('ifa', 'col.*')
brq2=brq2.select('ifa','state','city','geohash','latitude','longitude')

li=["Dhaka","Chittagong"]



brq2=brq2.filter(brq2.state .isin(li) )



df=aff.join( brq2,'ifa','inner')


df.printSchema()

df=df.select('ifa')


df=df.distinct()

df.count()


e_sim2=e_sim2.withColumnRenamed("ifa","Mobile Device ID")

e_sim2.printSchema()


e_sim2.coalesce(1).write.csv('s3a://ada-bd-emr/muntasir/E_SIM/Banglalink/', mode='overwrite', header=True)




s3a://ada-prod-data/etl/data/brq/sub/home-office/monthly/BD/2022{01}/*.parquet


# df.printSchema()

# data=df.select('name')

# data=data.distinct()


# data.show(20,truncate=False)