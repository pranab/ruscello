## Introduction
Real time clustered stream computation with Spark Streaming and Storm. The focus is Internet of Things (IoT)
real time analytic.

All the core algorithms are in the project hoidla, which is a plain java real time stream processing
library

## Philosophy
* Simple to use
* Extremely configurable with many configuration knobs

## Blogs
The following blogs of mine are good source of details. These are the only source
of detail documentation. 

* https://pkghosh.wordpress.com/2015/02/19/real-time-detection-of-outliers-in-sensor-data-using-spark-streaming/
* https://pkghosh.wordpress.com/2016/09/19/alarm-flooding-control-with-event-clustering-using-spark-streaming/


## Build
* Build hoidla:  mvn clean install; sbt publishLocal

* Build chombo-spark:  mvn clean install; sbt publishLocal

* Build ruscello:  sbt package


