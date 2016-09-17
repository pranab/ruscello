#!/bin/bash

PROJECT_HOME=/Users/pranab/Projects
JAR_NAME=$PROJECT_HOME/bin/ruscello/uber-ruscello-spark-1.0.jar
CLASS_NAME=org.ruscello.event.EventCluster
#MASTER=spark://PranabGoshsMBP2:7077
MASTER=spark://akash:7077

$SPARK_HOME/bin/spark-submit --class $CLASS_NAME    \
--conf spark.ui.killEnabled=true  --master $MASTER $JAR_NAME file:///Users/pranab/Projects/bin/ruscello/output/evc evtclust.conf
