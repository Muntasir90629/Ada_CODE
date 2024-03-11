muntasirul.hoque@ada-asia.com:~/environment $ pkg_list=com.databricks:spark-avro_2.11:4.0.0,org.apache.hadoop:hadoop-aws:2.7.1
muntasirul.hoque@ada-asia.com:~/environment $ pyspark --packages $pkg_list --executor-memory 39g --num-executors 29 --executor-cores 5 --conf "spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version=2"
Python 3.6.3 |Anaconda, Inc.| (default, Oct 13 2017, 12:02:49) 
[GCC 7.2.0] on linux
Type "help", "copyright", "credits" or "license" for more information.
Ivy Default Cache set to: /home/ec2-user/.ivy2/cache
The jars for the packages stored in: /home/ec2-user/.ivy2/jars
:: loading settings :: url = jar:file:/home/ec2-user/anaconda3/lib/python3.6/site-packages/pyspark/jars/ivy-2.4.0.jar!/org/apache/ivy/core/settings/ivysettings.xml
com.databricks#spark-avro_2.11 added as a dependency
org.apache.hadoop#hadoop-aws added as a dependency
:: resolving dependencies :: org.apache.spark#spark-submit-parent-4b1881aa-2e34-4376-b173-40609166610e;1.0
        confs: [default]
        found com.databricks#spark-avro_2.11;4.0.0 in central
        found org.slf4j#slf4j-api;1.7.5 in central
        found org.apache.avro#avro;1.7.6 in central
        found org.codehaus.jackson#jackson-core-asl;1.9.13 in central
        found org.codehaus.jackson#jackson-mapper-asl;1.9.13 in central
        found com.thoughtworks.paranamer#paranamer;2.3 in central
        found org.xerial.snappy#snappy-java;1.0.5 in central
        found org.apache.commons#commons-compress;1.4.1 in central
        found org.tukaani#xz;1.0 in central
        found org.apache.hadoop#hadoop-aws;2.7.1 in central
        found org.apache.hadoop#hadoop-common;2.7.1 in central
        found org.apache.hadoop#hadoop-annotations;2.7.1 in central
        found com.google.guava#guava;11.0.2 in central
        found com.google.code.findbugs#jsr305;3.0.0 in central
        found commons-cli#commons-cli;1.2 in central
        found org.apache.commons#commons-math3;3.1.1 in central
        found xmlenc#xmlenc;0.52 in central
        found commons-httpclient#commons-httpclient;3.1 in central
        found commons-logging#commons-logging;1.1.3 in central
        found commons-codec#commons-codec;1.4 in central
        found commons-io#commons-io;2.4 in central
        found commons-net#commons-net;3.1 in central
        found commons-collections#commons-collections;3.2.1 in central
        found javax.servlet#servlet-api;2.5 in central
        found org.mortbay.jetty#jetty;6.1.26 in central
        found org.mortbay.jetty#jetty-util;6.1.26 in central
        found com.sun.jersey#jersey-core;1.9 in central
        found com.sun.jersey#jersey-json;1.9 in central
        found org.codehaus.jettison#jettison;1.1 in central
        found com.sun.xml.bind#jaxb-impl;2.2.3-1 in central
        found javax.xml.bind#jaxb-api;2.2.2 in central
        found javax.xml.stream#stax-api;1.0-2 in central
        found javax.activation#activation;1.1 in central
        found org.codehaus.jackson#jackson-jaxrs;1.9.13 in central
        found org.codehaus.jackson#jackson-xc;1.9.13 in central
        found com.sun.jersey#jersey-server;1.9 in central
        found asm#asm;3.2 in central
        found log4j#log4j;1.2.17 in central
        found net.java.dev.jets3t#jets3t;0.9.0 in central
        found org.apache.httpcomponents#httpclient;4.2.5 in central
        found org.apache.httpcomponents#httpcore;4.2.5 in central
        found com.jamesmurty.utils#java-xmlbuilder;0.4 in central
        found commons-lang#commons-lang;2.6 in central
        found commons-configuration#commons-configuration;1.6 in central
        found commons-digester#commons-digester;1.8 in central
        found commons-beanutils#commons-beanutils;1.7.0 in central
        found commons-beanutils#commons-beanutils-core;1.8.0 in central
        found org.slf4j#slf4j-api;1.7.10 in central
        found com.google.protobuf#protobuf-java;2.5.0 in central
        found com.google.code.gson#gson;2.2.4 in central
        found org.apache.hadoop#hadoop-auth;2.7.1 in central
        found org.apache.directory.server#apacheds-kerberos-codec;2.0.0-M15 in central
        found org.apache.directory.server#apacheds-i18n;2.0.0-M15 in central
        found org.apache.directory.api#api-asn1-api;1.0.0-M20 in central
        found org.apache.directory.api#api-util;1.0.0-M20 in central
        found org.apache.zookeeper#zookeeper;3.4.6 in central
        found org.slf4j#slf4j-log4j12;1.7.10 in central
        found io.netty#netty;3.6.2.Final in central
        found org.apache.curator#curator-framework;2.7.1 in central
        found org.apache.curator#curator-client;2.7.1 in central
        found com.jcraft#jsch;0.1.42 in central
        found org.apache.curator#curator-recipes;2.7.1 in central
        found org.apache.htrace#htrace-core;3.1.0-incubating in central
        found javax.servlet.jsp#jsp-api;2.1 in central
        found jline#jline;0.9.94 in central
        found junit#junit;4.11 in central
        found org.hamcrest#hamcrest-core;1.3 in central
        found com.fasterxml.jackson.core#jackson-databind;2.2.3 in central
        found com.fasterxml.jackson.core#jackson-annotations;2.2.3 in central
        found com.fasterxml.jackson.core#jackson-core;2.2.3 in central
        found com.amazonaws#aws-java-sdk;1.7.4 in central
        found joda-time#joda-time;2.10.13 in central
        [2.10.13] joda-time#joda-time;[2.2,)
:: resolution report :: resolve 2158ms :: artifacts dl 36ms
        :: modules in use:
        asm#asm;3.2 from central in [default]
        com.amazonaws#aws-java-sdk;1.7.4 from central in [default]
        com.databricks#spark-avro_2.11;4.0.0 from central in [default]
        com.fasterxml.jackson.core#jackson-annotations;2.2.3 from central in [default]
        com.fasterxml.jackson.core#jackson-core;2.2.3 from central in [default]
        com.fasterxml.jackson.core#jackson-databind;2.2.3 from central in [default]
        com.google.code.findbugs#jsr305;3.0.0 from central in [default]
        com.google.code.gson#gson;2.2.4 from central in [default]
        com.google.guava#guava;11.0.2 from central in [default]
        com.google.protobuf#protobuf-java;2.5.0 from central in [default]
        com.jamesmurty.utils#java-xmlbuilder;0.4 from central in [default]
        com.jcraft#jsch;0.1.42 from central in [default]
        com.sun.jersey#jersey-core;1.9 from central in [default]
        com.sun.jersey#jersey-json;1.9 from central in [default]
        com.sun.jersey#jersey-server;1.9 from central in [default]
        com.sun.xml.bind#jaxb-impl;2.2.3-1 from central in [default]
        com.thoughtworks.paranamer#paranamer;2.3 from central in [default]
        commons-beanutils#commons-beanutils;1.7.0 from central in [default]
        commons-beanutils#commons-beanutils-core;1.8.0 from central in [default]
        commons-cli#commons-cli;1.2 from central in [default]
        commons-codec#commons-codec;1.4 from central in [default]
        commons-collections#commons-collections;3.2.1 from central in [default]
        commons-configuration#commons-configuration;1.6 from central in [default]
        commons-digester#commons-digester;1.8 from central in [default]
        commons-httpclient#commons-httpclient;3.1 from central in [default]
        commons-io#commons-io;2.4 from central in [default]
        commons-lang#commons-lang;2.6 from central in [default]
        commons-logging#commons-logging;1.1.3 from central in [default]
        commons-net#commons-net;3.1 from central in [default]
        io.netty#netty;3.6.2.Final from central in [default]
        javax.activation#activation;1.1 from central in [default]
        javax.servlet#servlet-api;2.5 from central in [default]
        javax.servlet.jsp#jsp-api;2.1 from central in [default]
        javax.xml.bind#jaxb-api;2.2.2 from central in [default]
        javax.xml.stream#stax-api;1.0-2 from central in [default]
        jline#jline;0.9.94 from central in [default]
        joda-time#joda-time;2.10.13 from central in [default]
        junit#junit;4.11 from central in [default]
        log4j#log4j;1.2.17 from central in [default]
        net.java.dev.jets3t#jets3t;0.9.0 from central in [default]
        org.apache.avro#avro;1.7.6 from central in [default]
        org.apache.commons#commons-compress;1.4.1 from central in [default]
        org.apache.commons#commons-math3;3.1.1 from central in [default]
        org.apache.curator#curator-client;2.7.1 from central in [default]
        org.apache.curator#curator-framework;2.7.1 from central in [default]
        org.apache.curator#curator-recipes;2.7.1 from central in [default]
        org.apache.directory.api#api-asn1-api;1.0.0-M20 from central in [default]
        org.apache.directory.api#api-util;1.0.0-M20 from central in [default]
        org.apache.directory.server#apacheds-i18n;2.0.0-M15 from central in [default]
        org.apache.directory.server#apacheds-kerberos-codec;2.0.0-M15 from central in [default]
        org.apache.hadoop#hadoop-annotations;2.7.1 from central in [default]
        org.apache.hadoop#hadoop-auth;2.7.1 from central in [default]
        org.apache.hadoop#hadoop-aws;2.7.1 from central in [default]
        org.apache.hadoop#hadoop-common;2.7.1 from central in [default]
        org.apache.htrace#htrace-core;3.1.0-incubating from central in [default]
        org.apache.httpcomponents#httpclient;4.2.5 from central in [default]
        org.apache.httpcomponents#httpcore;4.2.5 from central in [default]
        org.apache.zookeeper#zookeeper;3.4.6 from central in [default]
        org.codehaus.jackson#jackson-core-asl;1.9.13 from central in [default]
        org.codehaus.jackson#jackson-jaxrs;1.9.13 from central in [default]
        org.codehaus.jackson#jackson-mapper-asl;1.9.13 from central in [default]
        org.codehaus.jackson#jackson-xc;1.9.13 from central in [default]
        org.codehaus.jettison#jettison;1.1 from central in [default]
        org.hamcrest#hamcrest-core;1.3 from central in [default]
        org.mortbay.jetty#jetty;6.1.26 from central in [default]
        org.mortbay.jetty#jetty-util;6.1.26 from central in [default]
        org.slf4j#slf4j-api;1.7.10 from central in [default]
        org.slf4j#slf4j-log4j12;1.7.10 from central in [default]
        org.tukaani#xz;1.0 from central in [default]
        org.xerial.snappy#snappy-java;1.0.5 from central in [default]
        xmlenc#xmlenc;0.52 from central in [default]
        :: evicted modules:
        org.slf4j#slf4j-api;1.7.5 by [org.slf4j#slf4j-api;1.7.10] in [default]
        org.slf4j#slf4j-api;1.6.4 by [org.slf4j#slf4j-api;1.7.5] in [default]
        org.apache.avro#avro;1.7.4 by [org.apache.avro#avro;1.7.6] in [default]
        ---------------------------------------------------------------------
        |                  |            modules            ||   artifacts   |
        |       conf       | number| search|dwnlded|evicted|| number|dwnlded|
        ---------------------------------------------------------------------
        |      default     |   74  |   1   |   0   |   3   ||   71  |   0   |
        ---------------------------------------------------------------------

:: problems summary ::
:::: ERRORS
        SERVER ERROR: Bad Gateway url=https://dl.bintray.com/spark-packages/maven/joda-time/joda-time/maven-metadata.xml

        SERVER ERROR: Bad Gateway url=https://dl.bintray.com/spark-packages/maven/joda-time/joda-time/

        SERVER ERROR: Bad Gateway url=https://dl.bintray.com/spark-packages/maven/joda-time/joda-time/


:: USE VERBOSE OR DEBUG MESSAGE LEVEL FOR MORE DETAILS
:: retrieving :: org.apache.spark#spark-submit-parent-4b1881aa-2e34-4376-b173-40609166610e
        confs: [default]
        0 artifacts copied, 71 already retrieved (0kB/20ms)
22/02/28 06:42:43 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
Using Spark's default log4j profile: org/apache/spark/log4j-defaults.properties
Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
Welcome to
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /__ / .__/\_,_/_/ /_/\_\   version 2.4.6
      /_/

Using Python version 3.6.3 (default, Oct 13 2017 12:02:49)
SparkSession available as 'spark'.
>>> from pyspark import SparkContext, SparkConf, HiveContext
>>> from pyspark.sql.functions import *
>>> from pyspark.sql.types import *
>>> import pyspark.sql.functions as F
>>> import pyspark.sql.types as T
>>> import csv
>>> import pandas as pd
>>> import numpy as np
>>> import sys
>>> from pyspark.sql import Window
>>> from pyspark.sql.functions import rank, col
>>> import geohash2 as geohash
Traceback (most recent call last):
  File "<stdin>", line 1, in <module>
ModuleNotFoundError: No module named 'geohash2'
>>> import pygeohash as pgh
Traceback (most recent call last):
  File "<stdin>", line 1, in <module>
ModuleNotFoundError: No module named 'pygeohash'
>>> from functools import reduce
>>> from pyspark.sql import *
>>> df=spark.read.parquet('s3a://ada-prod-data/etl/data/brq/agg/agg_brq/monthly/BD/2022{01}/*.parquet')
>>> df.show()                                                                   
+--------------------+--------------------+--------------------+--------------------+--------------------+--------------------+-------------+---------+
|                 ifa|              device|          connection|                 app|             maxmind|                 gps|         user|brq_count|
+--------------------+--------------------+--------------------+--------------------+--------------------+--------------------+-------------+---------+
|00001f7e-8826-4f0...|[Symphony, Sympho...|[[Cable/DSL, X-li...|[[com.freevpninto...|[[wh0qcz4rt, 82, ...|[[wh0r1xg20, 1, 2...|    [, 0, -1]|       82|
|00004a51-1b84-47b...|[Samsung, Samsung...|[[Cellular, TELEN...|[[com.playit.vide...|[[wh0qfmj6c, 1, 2...|[[wh0qcpjhj, 1, 2...|      [,, -1]|       84|
|00006e38-9d6d-410...|[Tecno, Tecno Spa...|[[Cellular, Grame...|[[com.imo.android...|[[wh0r0dy4m, 1, 2...|                null|[F, 1990, 32]|        3|
|0000ab53-7d0f-4d9...|[Generic, Generic...|[[Cellular, Grame...|[[com.imo.android...|[[wh0r0rf23, 2, 2...|                null|    [, 0, -1]|       17|
|000109dd-33ca-40a...|[Vivo, Vivo Y91, ...|[[Cable/DSL, Skyl...|[[com.quantum.vid...|[[wh0rqgjud, 1, 2...|                null|   [O, 0, -1]|        1|
|00016916-f719-41f...|[Realme, Realme C...|[[Cellular, Bangl...|[[com.cyberlink.y...|[[wh0qcr6hc, 131,...|[[wh0r36gpq, 1, 2...|[F, 2000, 22]|      225|
|0001fcff-773c-47b...|[OPPO, OPPO A16, ...|[[Cellular, Bangl...|[[com.lenovo.anys...|[[w21z74m2p, 5, 2...|[[wh0r36gpq, 1, 2...|    [, 0, -1]|      128|
|00021b0d-bc07-452...|[Huawei, Huawei A...|[[Cable/DSL, Kazi...|[[io.yarsa.games....|[[wh2kweyw1, 67, ...|[[wh2kwgcru, 2, 2...|    [, 0, -1]|      125|
|0002207f-6958-43a...|[Samsung, Samsung...|[[Cable/DSL, Plus...|[[com.imo.android...|[[w5cr3sr47, 5, 2...|[[wh0zfg4zr, 1, 2...|      [,, -1]|        5|
|00023188-9410-4b3...|                null|[[Cellular, Grame...|[[com.montro.good...|[[wh0qcr6hc, 20, ...|[[wh0r1sndw, 5, 2...|   [F, 0, -1]|     1465|
|00023e54-903f-436...|[Xiaomi, Xiaomi R...|[[Cellular, Grame...|[[com.imo.android...|[[wh0r1hy5j, 17, ...|[[wh34f1uk2, 27, ...|      [,, -1]|      661|
|00024662-54c9-4ef...|                null|[[Cable/DSL, Litt...|[[com.water.balls...|[[wh0q76wq8, 64, ...|[[wh0q76wq8, 38, ...|    [, 0, -1]|       69|
|00024d75-ae37-4a7...|[Apple, Apple iPh...|[[Cable/DSL, Fast...|[[1413942319, 1, ...|[[wh0r3rf6b, 1, 2...|[[wh0r3rf6b, 1, 2...|    [, 0, -1]|        1|
|000274c9-9cf1-417...|                null|[[Cable/DSL, Chit...|[[com.camerasidea...|[[wh033gvm8, 45, ...|[[wh030kykv, 5, 2...|      [,, -1]|       45|
|00031444-83c6-4d9...|                null|[[Cellular, Bangl...|[[com.playit.vide...|[[wh0r0dy4m, 5, 2...|[[wh0r0dy4m, 1, 2...|    [, 0, -1]|        7|
|00033726-bb85-43c...|[Realme, Realme C...|[[Cellular, Grame...|[[videoplayer.vid...|[[wh0r1sndw, 1, 2...|[[wh0qcr6hc, 9, 2...|      [,, -1]|       62|
|00034f9c-9bf8-4d5...|[Vivo, Vivo Y20 2...|[[Cellular, TELEN...|[[com.imo.android...|[[wh0r0rf23, 20, ...|                null|    [, 0, -1]|       23|
|00045c0b-a02f-496...|                null|[[Cellular, DeVot...|[[com.playit.vide...|[[wh0r36gpq, 3, 2...|[[wh0r1sndw, 2, 2...|    [, 0, -1]|       33|
|00049382-9821-40d...|[OPPO, OPPO Oppo ...|[[Cable/DSL, Best...|[[com.git.carromk...|[[wh0qbdb2z, 25, ...|[[wh0fzpvtv, 1, 2...|[F, 1977, 45]|      168|
|0004b577-896b-4bc...|[Realme, Realme C...|[[Cellular, Robi,...|[[com.hippo.qrcod...|[[wh0qbdb2z, 106,...|[[wh0qcpjhj, 1, 2...|    [, 0, -1]|      141|
+--------------------+--------------------+--------------------+--------------------+--------------------+--------------------+-------------+---------+
only showing top 20 rows

>>> ifa_data=df.select('ifa',F.explode('gps.geohash').alias('geohash'))
>>> 
>>> ifa_data.withColumn("new_geohash", substring(col("geohash"),1,6))
DataFrame[ifa: string, geohash: string, new_geohash: string]
>>> 
>>> ifa_new=ifa_data.withColumn("new_geohash", substring(col("geohash"),1,6))
>>> ifa_new.show()
+--------------------+---------+-----------+                                    
|                 ifa|  geohash|new_geohash|
+--------------------+---------+-----------+
|00001f7e-8826-4f0...|wh0r1xg20|     wh0r1x|
|00001f7e-8826-4f0...|wh0r1s538|     wh0r1s|
|00001f7e-8826-4f0...|wh0r1s52b|     wh0r1s|
|00001f7e-8826-4f0...|wh0r1xg28|     wh0r1x|
|00001f7e-8826-4f0...|wh0r1xeq0|     wh0r1x|
|00001f7e-8826-4f0...|wh0qcz4rt|     wh0qcz|
|00001f7e-8826-4f0...|wh0r1egr8|     wh0r1e|
|00004a51-1b84-47b...|wh0qcpjhj|     wh0qcp|
|00004a51-1b84-47b...|wh0qcr6hc|     wh0qcr|
|00004a51-1b84-47b...|wh1494p8r|     wh1494|
|00004a51-1b84-47b...|wh0r1ccgx|     wh0r1c|
|00004a51-1b84-47b...|wh0fzvg0n|     wh0fzv|
|00004a51-1b84-47b...|wh0qfmj6c|     wh0qfm|
|00004a51-1b84-47b...|wh0r3rf6b|     wh0r3r|
|00004a51-1b84-47b...|wh0qbdb2z|     wh0qbd|
|00004a51-1b84-47b...|wh0r1sndw|     wh0r1s|
|00004a51-1b84-47b...|wh14bkuc4|     wh14bk|
|00004a51-1b84-47b...|wh0r0rf23|     wh0r0r|
|00016916-f719-41f...|wh0r36gpq|     wh0r36|
|00016916-f719-41f...|wh0r0dy4m|     wh0r0d|
+--------------------+---------+-----------+
only showing top 20 rows

>>> ifa_new=ifa_new.drop(ifa_new.geohash)
>>> 
>>> ifa_new.printSchema()
root
 |-- ifa: string (nullable = true)
 |-- new_geohash: string (nullable = true)

>>> 
>>> ifa_new=ifa_new.withColumnRenamed("new_geohash","geohash")
>>> ifa_new.show()
+--------------------+-------+                                                  
|                 ifa|geohash|
+--------------------+-------+
|00001f7e-8826-4f0...| wh0r1x|
|00001f7e-8826-4f0...| wh0r1s|
|00001f7e-8826-4f0...| wh0r1s|
|00001f7e-8826-4f0...| wh0r1x|
|00001f7e-8826-4f0...| wh0r1x|
|00001f7e-8826-4f0...| wh0qcz|
|00001f7e-8826-4f0...| wh0r1e|
|00004a51-1b84-47b...| wh0qcp|
|00004a51-1b84-47b...| wh0qcr|
|00004a51-1b84-47b...| wh1494|
|00004a51-1b84-47b...| wh0r1c|
|00004a51-1b84-47b...| wh0fzv|
|00004a51-1b84-47b...| wh0qfm|
|00004a51-1b84-47b...| wh0r3r|
|00004a51-1b84-47b...| wh0qbd|
|00004a51-1b84-47b...| wh0r1s|
|00004a51-1b84-47b...| wh14bk|
|00004a51-1b84-47b...| wh0r0r|
|00016916-f719-41f...| wh0r36|
|00016916-f719-41f...| wh0r0d|
+--------------------+-------+
only showing top 20 rows

>>> master=spark.read.csv('s3a://ada-bd-emr/muntasir/POI/bd_poi_master_v7.0.csv',header=True)
>>> master_new=master.select('level_id','name','geohash')
>>> master_new.show()
+-------------+--------------------+-------+
|     level_id|                name|geohash|
+-------------+--------------------+-------+
|ADV_03_09_001|Sahebganj Footbal...| tuxswf|
|ADV_03_09_001|Katakhali Playground| wh2nbb|
|ADV_03_09_001|Cheuria Mondolpar...| tupruy|
|ADV_03_09_001|SAHEB GANJ VC FOO...| tuxswf|
|ADV_03_09_001| BPL Ground, Bhaluka| wh2p35|
|ADV_03_09_001|Aklash Shibpur Sh...| turqhf|
|ADV_03_09_001|Shafia Sharif Spo...| wh0h10|
|ADV_03_09_001|Manikgonj Hock St...| tup6uc|
|ADV_03_09_001| Playground Abdullah| tursqz|
|ADV_03_09_001|Khailsindur play ...| wh20c5|
|ADV_03_09_001| Batason Play Ground| tux3sg|
|ADV_03_09_001|Hatshira Purbopar...| tursnz|
|ADV_03_09_001|          Balur math| wh00cp|
|ADV_03_09_001|Bakshigonj Primar...| tux7hz|
|ADV_03_09_001|Halipad playgroun...| tgzxyf|
|ADV_03_09_001|Bukabunia School ...| w5bncn|
|ADV_03_09_001|Churamankathi Hig...| tupkj4|
|ADV_03_09_001|      EPZ Playground| tgzxyv|
|ADV_03_09_001|Bhandaria Shishu ...| w5bpcm|
|ADV_03_09_001|       CP Playground| turkj1|
+-------------+--------------------+-------+
only showing top 20 rows

>>> master_new=master_new.withColumn("level_id_new", substring(col("level_id"),1,3))
>>> 
>>> master_new.show()
+-------------+--------------------+-------+------------+
|     level_id|                name|geohash|level_id_new|
+-------------+--------------------+-------+------------+
|ADV_03_09_001|Sahebganj Footbal...| tuxswf|         ADV|
|ADV_03_09_001|Katakhali Playground| wh2nbb|         ADV|
|ADV_03_09_001|Cheuria Mondolpar...| tupruy|         ADV|
|ADV_03_09_001|SAHEB GANJ VC FOO...| tuxswf|         ADV|
|ADV_03_09_001| BPL Ground, Bhaluka| wh2p35|         ADV|
|ADV_03_09_001|Aklash Shibpur Sh...| turqhf|         ADV|
|ADV_03_09_001|Shafia Sharif Spo...| wh0h10|         ADV|
|ADV_03_09_001|Manikgonj Hock St...| tup6uc|         ADV|
|ADV_03_09_001| Playground Abdullah| tursqz|         ADV|
|ADV_03_09_001|Khailsindur play ...| wh20c5|         ADV|
|ADV_03_09_001| Batason Play Ground| tux3sg|         ADV|
|ADV_03_09_001|Hatshira Purbopar...| tursnz|         ADV|
|ADV_03_09_001|          Balur math| wh00cp|         ADV|
|ADV_03_09_001|Bakshigonj Primar...| tux7hz|         ADV|
|ADV_03_09_001|Halipad playgroun...| tgzxyf|         ADV|
|ADV_03_09_001|Bukabunia School ...| w5bncn|         ADV|
|ADV_03_09_001|Churamankathi Hig...| tupkj4|         ADV|
|ADV_03_09_001|      EPZ Playground| tgzxyv|         ADV|
|ADV_03_09_001|Bhandaria Shishu ...| w5bpcm|         ADV|
|ADV_03_09_001|       CP Playground| turkj1|         ADV|
+-------------+--------------------+-------+------------+
only showing top 20 rows

>>> airport=master_new.filter(master_new.level_id_new=='APT')
>>> airport.show()
+-------------+--------------------+-------+------------+
|     level_id|                name|geohash|level_id_new|
+-------------+--------------------+-------+------------+
|APT_01_01_010|     Barisal Airport| wh0ctw|         APT|
|APT_01_01_019|     Jashore Airport| tup7ug|         APT|
|APT_01_01_043|     Saidpur Airport| tux4x4|         APT|
|APT_01_01_045|Shah Makhdum Airport| tur506|         APT|
|APT_02_01_012| Cox's Bazar Airport| w5c6hb|         APT|
|APT_02_01_022|Hazrat Shahjalal ...| wh0r9h|         APT|
|APT_02_01_044|Osmani Internatio...| wh3mfx|         APT|
|APT_02_01_052|Shah Amanat Inter...| w5cq93|         APT|
+-------------+--------------------+-------+------------+

>>> APT=airport.join(ifa_new,"geohash","left")
>>> APT.count()
37140  