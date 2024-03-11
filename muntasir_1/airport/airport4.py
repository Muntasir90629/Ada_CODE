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
from pyspark.sql.functions import date_format




# for i in range(11,18):
#     url='s3a://ada-prod-data/etl/data/brq/agg/agg_brq/daily/BD/20220501'
#     df=spark.read.parquet(url+'/*.parquet')
#     df2=df.select('ifa',explode('gps')).select('ifa','col.*')
#     df3=df2.select('ifa','geohash','last_seen')
#     df3= df3.withColumn("date", date_format('last_seen', 'dd'))
#     df3=df3.withColumn("new_geohash", substring(col("geohash"),1,6))
#     # ifa_new.show()
#     df3=df3.drop(df3.geohash)
#     df3=df3.withColumnRenamed("new_geohash","geohash")
#     # df3.printSchema()
#     #airport CSV
#     main_airport=spark.read.csv('s3a://ada-bd-emr/muntasir/airport/airport.csv',header=True)
#     # main_airport=main_airport.drop(main_airport.geohash)
#     # main_airport=main_airport.withColumnRenamed("geohash9","geohash")
#     # main_airport.show(truncate=False
#     main_airport=main_airport.select('name','geohash9')
#     main_airport=main_airport.withColumn("geohash", substring(col("geohash9"),1,6))
#     main_airport=main_airport.drop(main_airport.geohash9)
#     #dhaka-ctg count
#     dhaka_airport=main_airport.filter(main_airport.name == 'Hazrat Shahjalal International Airport')
    
#     dhaka_airport_visitors=dhaka_airport.join(df3,'geohash','left')
    
#     dhaka_air_ifa=dhaka_airport_visitors.select('ifa')
    
    
    
    
#     ctg_airport=main_airport.filter(main_airport.name == 'Shah Amanat International Airport')
    
#     ctg_airport__visitors=ctg_airport.join(df3,'geohash','left')
    
#     ctg_air_ifa=ctg_airport__visitors.select('ifa')
    
    
#     # ctg_air_ifa.distinct()
    
    
    
#     common_ifa=dhaka_air_ifa.join(ctg_air_ifa,'ifa','inner')
    
#     common_ifa.distinct().count()
    
       
        
        
        
#     # if(len(i)%2==1):
#     #     url='s3a://ada-prod-data/etl/data/brq/agg/agg_brq/daily/BD/2022050'
        
        
    
    
df=spark.read.parquet('s3a://ada-prod-data/etl/data/brq/agg/agg_brq/daily/BD/2022{0507}/*.parquet')
    
    
    
df2=df.select('ifa',explode('gps')).select('ifa','col.*')
    
df3=df2.select('ifa','geohash','last_seen')
    
    
    
df3= df3.withColumn("date", date_format('last_seen', 'dd'))
    
df3=df3.withColumn("new_geohash", substring(col("geohash"),1,6))
    
    # ifa_new.show()
    
df3=df3.drop(df3.geohash)
    
df3=df3.withColumnRenamed("new_geohash","geohash")
    
df3.printSchema()
    
    
    
    #airport CSV
    
main_airport=spark.read.csv('s3a://ada-bd-emr/muntasir/airport/airport.csv',header=True)


# main_airport=main_airport.drop(main_airport.geohash)

# main_airport=main_airport.withColumnRenamed("geohash9","geohash")

# main_airport.show(truncate=False)


main_airport=main_airport.select('name','geohash9')

main_airport=main_airport.withColumn("geohash", substring(col("geohash9"),1,6))

main_airport=main_airport.drop(main_airport.geohash9)


#dhaka-ctg count

dhaka_airport=main_airport.filter(main_airport.name == 'Hazrat Shahjalal International Airport')

dhaka_airport_visitors=dhaka_airport.join(df3,'geohash','left')

dhaka_air_ifa=dhaka_airport_visitors.select('ifa')




ctg_airport=main_airport.filter(main_airport.name == 'Shah Amanat International Airport')

ctg_airport__visitors=ctg_airport.join(df3,'geohash','left')

ctg_air_ifa=ctg_airport__visitors.select('ifa')


# ctg_air_ifa.distinct()



common_ifa=dhaka_air_ifa.join(ctg_air_ifa,'ifa','inner')

common_ifa.distinct().count()


#dhaka-sylhet count





# ---------------------------------------------------------------------------------------------------------------------------------------------


dhaka_airport=main_airport.filter(main_airport.name == 'Hazrat Shahjalal International Airport')

dhaka_airport_visitors=dhaka_airport.join(df3,'geohash','left')

dhaka_air_ifa=dhaka_airport_visitors.select('ifa','geohash')




sylhet_airport=main_airport.filter(main_airport.name == 'Osmani International Airport, Sylhet')

sylhet_airport__visitors=sylhet_airport.join(df3,'geohash','left')

sylhet_air_ifa=sylhet_airport__visitors.select('ifa','geohash')



common_ifa=dhaka_air_ifa.join(sylhet_air_ifa,'ifa','inner')

common_ifa.distinct().count()


#dhaka-Coxbazar airport



dhaka_airport=main_airport.filter(main_airport.name == 'Hazrat Shahjalal International Airport')

dhaka_airport_visitors=dhaka_airport.join(df3,'geohash','left')

dhaka_air_ifa=dhaka_airport_visitors.select('ifa','geohash')




cox_airport=main_airport.filter(main_airport.geohash == 'w5c6hb')

cox_airport__visitors=cox_airport.join(df3,'geohash','left')

cox_air_ifa=cox_airport__visitors.select('ifa','geohash')



common_ifa=dhaka_air_ifa.join(cox_air_ifa,'ifa','inner')

common_ifa.distinct().count()


#dhaka-barishal airport



dhaka_airport=main_airport.filter(main_airport.name == 'Hazrat Shahjalal International Airport')

dhaka_airport_visitors=dhaka_airport.join(df3,'geohash','left')

dhaka_air_ifa=dhaka_airport_visitors.select('ifa','geohash')




barishal_airport=main_airport.filter(main_airport.name == 'Barisal Airport')

barishal_airport__visitors=barishal_airport.join(df3,'geohash','left')

barishal_air_ifa=barishal_airport__visitors.select('ifa','geohash')



common_ifa=dhaka_air_ifa.join(barishal_air_ifa,'ifa','inner')

common_ifa.distinct().count()


#dhaka-Jashore airport


dhaka_airport=main_airport.filter(main_airport.name == 'Hazrat Shahjalal International Airport')

dhaka_airport_visitors=dhaka_airport.join(df3,'geohash','left')

dhaka_air_ifa=dhaka_airport_visitors.select('ifa','geohash')




Jashor_airport=main_airport.filter(main_airport.name == 'Jashore Airport')

Jashor_airport__visitors=Jashor_airport.join(df3,'geohash','left')

Jashor_air_ifa=Jashor_airport__visitors.select('ifa','geohash')



common_ifa=dhaka_air_ifa.join(Jashor_air_ifa,'ifa','inner')

common_ifa.distinct().count()


#dhaka-Saidpur airport


dhaka_airport=main_airport.filter(main_airport.name == 'Hazrat Shahjalal International Airport')

dhaka_airport_visitors=dhaka_airport.join(df3,'geohash','left')

dhaka_air_ifa=dhaka_airport_visitors.select('ifa','geohash')




Saidpur_airport=main_airport.filter(main_airport.name == 'Saidpur Airport')

Saidpur_airport__visitors=Saidpur_airport.join(df3,'geohash','left')

Saidpur_air_ifa=Saidpur_airport__visitors.select('ifa','geohash')



common_ifa=dhaka_air_ifa.join(Saidpur_air_ifa,'ifa','inner')

common_ifa.distinct().count()


#dhaka - rajshahi airport


dhaka_airport=main_airport.filter(main_airport.name == 'Hazrat Shahjalal International Airport')

dhaka_airport_visitors=dhaka_airport.join(df3,'geohash','left')

dhaka_air_ifa=dhaka_airport_visitors.select('ifa','geohash')




rajshahi_airport=main_airport.filter(main_airport.name == 'Shah Makhdum Airport')

rajshahi_airport.show(trucate=False)

rajshahi_airport__visitors=rajshahi_airport.join(df3,'geohash','left')

rajshahi_air_ifa=rajshahi_airport__visitors.select('ifa','geohash')



common_ifa=dhaka_air_ifa.join(rajshahi_air_ifa,'ifa','inner')

common_ifa.distinct().count()




