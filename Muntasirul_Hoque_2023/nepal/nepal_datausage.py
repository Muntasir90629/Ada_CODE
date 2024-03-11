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



brq = spark.read.parquet('s3a://ada-prod-data/etl/data/brq/agg/agg_brq/monthly/NP/{202211,202212,202301}/*.parquet')
df=brq.select('ifa','brq_count')
brq_avg =df.agg({'brq_count': 'avg'})
c = brq_avg.collect()[0][0]
print(c)
final_df = df.withColumn("data_usage", when(df.brq_count > c+390,"ULTRA HIGH")\
.when(df.brq_count >= c+130,"HIGH")\
.when(df.brq_count < c-70,"LOW")\
.when(df.brq_count < c+130,"MID")\
.otherwise(df.brq_count))
output=final_df.groupBy('data_usage').agg(F.countDistinct('ifa').alias('count')).sort('count', ascending=False)
output.show(4, False)




brq = spark.read.parquet('s3a://ada-prod-data/etl/data/brq/agg/agg_brq/monthly/NP/{202211,202212,202301}/*.parquet')
age_df=brq.select('ifa','user.age')

age_df2 = age_df.withColumn("Prediction", when(age_df.age >= 50 ,"50+")\
.when(age_df.age >35 ,"35-49")\
.when(age_df.age >=25 ,"25-34")\
.when(age_df.age >1,"18-24")\
.when(age_df.age <=-1,"Bellow 18")\
.otherwise(age_df.age))


age_df2.printSchema()



Nepal_data=final_df.join(age_df2,'ifa','left')


Nepal_data=Nepal_data.filter(Nepal_data.Prediction == '50+')


Nepal_data.show(100)

output=Nepal_data.groupBy('data_usage').agg(F.countDistinct('ifa').alias('count')).sort('count', ascending=False)
output.show(4, False)





output=age_df2.groupBy('Prediction').agg(F.countDistinct('ifa').alias('count')).sort('count', ascending=False)
output.show(5, False)

# brq = spark.read.parquet('s3a://ada-prod-data/etl/data/brq/agg/agg_brq/monthly/NP/{202301}/*.parquet')

# brq=brq.select('ifa')

# brq=brq.distinct()

# brq.count()



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


brq = spark.read.parquet('s3a://ada-prod-data/etl/data/brq/sub/age/monthly/BD/{202204}/*.parquet')



