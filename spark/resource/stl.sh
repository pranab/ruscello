#!/bin/bash

PROJECT_HOME=/Users/pranab/Projects
CHOMBO_JAR_NAME=$PROJECT_HOME/bin/chombo/uber-chombo-spark-1.0.jar
RUSCELLO_JAR_NAME=$PROJECT_HOME/bin/ruscello/uber-ruscello-spark-1.0.jar
MASTER=spark://akash:7077

case "$1" in

"numStat")
	echo "running NumericalAttrStats Spark job"
	CLASS_NAME=org.chombo.spark.explore.NumericalAttrStats
	INPUT=file:///Users/pranab/Projects/bin/ruscello/input/stl/*
	OUTPUT=file:///Users/pranab/Projects/bin/ruscello/output/mea
	rm -rf ./output/mea
	$SPARK_HOME/bin/spark-submit --class $CLASS_NAME   \
	--conf spark.ui.killEnabled=true --master $MASTER $CHOMBO_JAR_NAME  $INPUT $OUTPUT stl.conf
	rm -rf ./output/mea/_SUCCESS
	ls -l ./output/mea/
;;

"cpStat")
	echo "copying stats files"
	STAT_FILES=$PROJECT_HOME/bin/ruscello/output/mea/*
	OTHER_DIR=$PROJECT_HOME/bin/ruscello/other/stl
	cp /dev/null $OTHER_DIR/stats.txt
	for f in $STAT_FILES
	do
  		echo "Copying file $f ..."
  		cat $f >> $OTHER_DIR/stats.txt
	done
	ls -l $OTHER_DIR
;;

"autoCor")
	echo "running AutoCorrelation Spark job"
	CLASS_NAME=org.ruscello.explore.AutoCorrelation
	INPUT=file:///Users/pranab/Projects/bin/ruscello/input/stl/*
	OUTPUT=file:///Users/pranab/Projects/bin/ruscello/output/auc
	rm -rf ./output/auc
	$SPARK_HOME/bin/spark-submit --class $CLASS_NAME   \
	--conf spark.ui.killEnabled=true --master $MASTER $RUSCELLO_JAR_NAME  $INPUT $OUTPUT stl.conf
	rm -rf ./output/auc/_SUCCESS
	ls -l ./output/auc/
;;

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