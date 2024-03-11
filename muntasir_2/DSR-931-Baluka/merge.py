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



df1= spark.read.csv('s3a://ada-dev/BD-DataScience/muntasir/Baluka_DSR_931/geo_7/df1/*.csv',header=True)
df1.count()
df1.printSchema()

df2= spark.read.csv('s3a://ada-dev/BD-DataScience/muntasir/Baluka_DSR_931/geo_7/df2/*.csv',header=True)
df2.count()
df2.printSchema()

df3= spark.read.csv('s3a://ada-dev/BD-DataScience/muntasir/Baluka_DSR_931/geo_7/df3/*.csv',header=True)
df3.count()
df3.printSchema()

df4= spark.read.csv('s3a://ada-dev/BD-DataScience/muntasir/Baluka_DSR_931/geo_7/df4/*.csv',header=True)

df4.count()
df4.printSchema()

df5= spark.read.csv('s3a://ada-dev/BD-DataScience/muntasir/Baluka_DSR_931/geo_7/df5/*.csv',header=True)

df5.count()
df5.printSchema()

df6= spark.read.csv('s3a://ada-dev/BD-DataScience/muntasir/Baluka_DSR_931/geo_7/df6/*.csv',header=True)
df6.count()
df6.printSchema()

df7= spark.read.csv('s3a://ada-dev/BD-DataScience/muntasir/Baluka_DSR_931/geo_7/df7/*.csv',header=True)
df7.count()
df7.printSchema()

df8= spark.read.csv('s3a://ada-dev/BD-DataScience/muntasir/Baluka_DSR_931/geo_7/df8/*.csv',header=True)
df8.count()
df8.printSchema()

df9= spark.read.csv('s3a://ada-dev/BD-DataScience/muntasir/Baluka_DSR_931/geo_7/df9/*.csv',header=True)
df9.count()
df9.printSchema()

df10= spark.read.csv('s3a://ada-dev/BD-DataScience/muntasir/Baluka_DSR_931/geo_7/df10/*.csv',header=True)
df10.count()
df10.printSchema()

df11= spark.read.csv('s3a://ada-dev/BD-DataScience/muntasir/Baluka_DSR_931/geo_7/df11/*.csv',header=True)
df11.count()
df11.printSchema()

df12= spark.read.csv('s3a://ada-dev/BD-DataScience/muntasir/Baluka_DSR_931/geo_7/df12/*.csv',header=True)
df12.count()
df12.printSchema()

df13= spark.read.csv('s3a://ada-dev/BD-DataScience/muntasir/Baluka_DSR_931/geo_7/df13/*.csv',header=True)

df13.count()
df13.printSchema()

df14= spark.read.csv('s3a://ada-dev/BD-DataScience/muntasir/Baluka_DSR_931/geo_7/df14/*.csv',header=True)
df14.count()
df14.printSchema()


from functools import reduce
from pyspark.sql import DataFrame



dfs = [df1, df2, df3 ,df4,df6,df7,df8,df9,df10,df11,df12,df13,df14]

# create merged dataframe
df_complete = reduce(DataFrame.unionAll, dfs)

df_complete =df_complete.distinct()

df_complete.count()


s3://ada-dev/BD-DataScience/muntasir/Baluka_DSR_931/total_ifa/



df_complete.coalesce(1).write.csv('s3a://ada-dev/BD-DataScience/muntasir/Baluka_DSR_931/total_ifa/', mode='overwrite', header=True)


dfs = [df_complete, df4]

# create merged dataframe
df_complete1 = reduce(DataFrame.unionAll, dfs)



df_complete1.count()





df=df1.join( df2,'ifa','outer')

df.count()


df=df.join( df3,'ifa','outer')

df.count()


df=df.join( df4,'ifa','outer')

df.count()


df=df.join( df5,'ifa','outer')

df.count()






df.printSchema()











from functools import reduce

from pyspark.sql import DataFrame



dfs = [df1,df2,df3,df4,df5,df6,df7,df8,df9,df10,df11,df12,df13,df14]

# create merged dataframe
df_complete = reduce(DataFrame.unionAll, dfs)



dfs = [df1,df2,df3,df4,df5,df6,df7,df8,df9,df10,df11,df12,df13,df14]


dfs=dfs.select('ifa')

dfs=dfs.distinct()

dfs.count()