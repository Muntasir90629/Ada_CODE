from pyspark import SparkContext, SparkConf, HiveContext
from pyspark.sql.functions import *
import pyspark.sql.functions as F
import sys
country='BD'
time='monthly'
m='202001'

df = spark.read.parquet('s3a://ada-prod-data/etl/data/brq/sub/connection/monthly/'+country+'/'+m+'/*.parquet')

df2 = df.select('ifa', explode('mm_connection')).select('ifa', 'col.*')
df3 = df2.select('ifa', 'mm_con_type_desc', 'mm_carrier_name')
geo = spark.read.parquet('s3a://ada-bd-emr/result/2022/Robi/DSR-623/raw/bl/'+m+'/*.parquet')
geo.printSchema()
root
 |-- ifa: string (nullable = true)
 |-- telco: string (nullable = true)
 
  df4=geo.join(df3,on='ifa',how='left')
  
  df4.printSchema()
root
 |-- ifa: string (nullable = true)
 |-- telco: string (nullable = true)
 |-- mm_con_type_desc: string (nullable = true)
 |-- mm_carrier_name: string (nullable = true)
 
 dual_simmers = df4.filter(col('mm_con_type_desc') == 'Cellular').select('ifa', 'mm_carrier_name').distinct()
 
 carrier_name=df4.filter(col('mm_con_type_desc') == 'Cellular').select('ifa', 'mm_carrier_name').distinct()
 dual_simmers = dual_simmers.groupBy('ifa').agg(countDistinct('mm_carrier_name').alias('sims'))
 dual_simmers = dual_simmers.filter(col('sims') > 1).withColumn('dual_sim', F.lit(1))
 sims_12=dual_simmers.filter(col('sims')==12).withColumn('sims_12',F.lit(12))
 
#  |21aa3680-f322-4ce2-af73-0cdcb186bc26|Fiber@Home Global Limited,Chittagong Online Limited AS38592 AP,Robi,Agni Systems Limited,Bangladesh Online,EARTH TELECOMMUNICATION (Pvt),GrameenPhone,Telecom Operator & Internet Service Provider as we,Dtech Limited,APNIC Debogon Project,aamra networks limited,Banglalink|
# |a8c00cfb-80e7-4af3-b4a8-61b127c08a3f|HZ Hosting Ltd,RockLab LLC,NTX Technologies s.r.o.,Robi,DeVoteD NBN,Melbikomas UAB,TORAT Private Enterprise,GrameenPhone,LLC Baxet,Transcom LLC,Reliable Communications s.r.o.,Telecom Operator & Internet Service Provider as we   
 
 carrier_name.select('mm_carrier_name').distinct().show(truncate=False)
# +------------------------------+                                                
# |mm_carrier_name               |
# +------------------------------+
# |Netsys Global Telecom Limited |
# |Reliable Communications s.r.o.|
# |HeiTech Padu Bhd.             |
# |Verizon Nederland B.V.        |
# |CiTYCOM Network               |
# |TOKAI                         |
# |Orange Cote d'Ivoire          |
# |X-Host SRL                    |
# |HostHatch                     |
# |Alpha Broadway System         |
# |Hong Kong Broadband Network   |
# |Netforest,Inc.                |
# |Hathway                       |
# |Nagasaki Cable Media          |
# |Vodafone New Zealand          |
# |Broad Band Telecom Services   |
# |stc Bahrain                   |
# |Internap Corporation          |
# |Phillips-Van Heusen           |
# |Akceycom Limited.             |
# +------------------------------+

