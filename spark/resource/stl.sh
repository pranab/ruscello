#!/bin/bash

PROJECT_HOME=/Users/pranab/Projects
CHOMBO_JAR_NAME=$PROJECT_HOME/bin/chombo/uber-chombo-spark-1.0.jar
RUSCELLO_JAR_NAME=$PROJECT_HOME/bin/ruscello/uber-ruscello-spark-1.0.jar
MASTER=spark://akash:7077

case "$1" in

"stl")
	echo "running StlDecomposition Spark job"
	CLASS_NAME=org.ruscello.etl.StlDecomposition
	INPUT=file:///Users/pranab/Projects/bin/ruscello/input/stl/*
	OUTPUT=file:///Users/pranab/Projects/bin/ruscello/output/stl
	rm -rf ./output/stl
	$SPARK_HOME/bin/spark-submit --class $CLASS_NAME   \
	--conf spark.ui.killEnabled=true --master $MASTER $RUSCELLO_JAR_NAME  $INPUT $OUTPUT stl.conf
;;

*) 
	echo "unknown operation $1"
	;;

esac