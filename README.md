## Introduction
Real time time and offline time series analysis with Spark, Spark Streaming and Storm. useful 
for Internet of Things (IoT).

All the core algorithms are in the project hoidla, which is a plain java time series processing
library

## Philosophy
* Simple to use
* Extremely configurable with many configuration knobs

## Blogs
The following blogs of mine are good source of details. These are the only source
of detail documentation. 

* https://pkghosh.wordpress.com/2015/02/19/real-time-detection-of-outliers-in-sensor-data-using-spark-streaming/
* https://pkghosh.wordpress.com/2016/09/19/alarm-flooding-control-with-event-clustering-using-spark-streaming/
* https://pkghosh.wordpress.com/2018/12/23/time-series-seasonal-cycle-detection-with-auto-correlation-on-spark/


## Build
Please refer to spark_dpendency.txt for details

* Build hoidla:  mvn clean install; sbt publishLocal

* Build chombo:  mvn clean install; sbt publishLocal

* Build chombo-spark:  sbt package; sbt publishLocal

* Build ruscello:  sbt package


