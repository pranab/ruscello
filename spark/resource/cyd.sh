#!/bin/bash

PROJECT_HOME=/Users/pranab/Projects
CHOMBO_JAR_NAME=$PROJECT_HOME/bin/chombo/uber-chombo-spark-1.0.jar
RUSCELLO_JAR_NAME=$PROJECT_HOME/bin/ruscello/uber-ruscello-spark-1.0.jar
MASTER=spark://akash:7077

case "$1" in

"numStat")
	echo "running NumericalAttrStats Spark job"
	CLASS_NAME=org.chombo.spark.explore.NumericalAttrStats
	INPUT=file:///Users/pranab/Projects/bin/ruscello/input/teg/eusage.txt
	OUTPUT=file:///Users/pranab/Projects/bin/ruscello/output/mea
	rm -rf ./output/mea
	$SPARK_HOME/bin/spark-submit --class $CLASS_NAME   \
	--conf spark.ui.killEnabled=true --master $MASTER $CHOMBO_JAR_NAME  $INPUT $OUTPUT cyd.conf
;;

"crStatsFile")
	echo "copying and consolidating stats file"
	cat $PROJECT_HOME/bin/ruscello/output/mea/part-00000 > $PROJECT_HOME/bin/ruscello/other/auc/stats.txt
	cat $PROJECT_HOME/bin/ruscello/output/mea/part-00001 >> $PROJECT_HOME/bin/ruscello/other/auc/stats.txt
	ls -l $PROJECT_HOME/bin/ruscello/other/auc
;;

"tempAggr")
	echo "running TemporalAggregator Spark job"
	CLASS_NAME=org.ruscello.explore.TemporalAggregator
	INPUT=file:///Users/pranab/Projects/bin/ruscello/input/teg/eusage.txt
	OUTPUT=file:///Users/pranab/Projects/bin/ruscello/output/teg
	rm -rf ./output/teg
	$SPARK_HOME/bin/spark-submit --class $CLASS_NAME   \
	--conf spark.ui.killEnabled=true --master $MASTER $RUSCELLO_JAR_NAME  $INPUT $OUTPUT cyd.conf
;;

"crAucInput")
	echo "copying and consolidating tem aggregation output file"
	cat $PROJECT_HOME/bin/ruscello/output/teg/part-00000 > $PROJECT_HOME/bin/ruscello/input/auc/eusage.txt
	cat $PROJECT_HOME/bin/ruscello/output/teg/part-00001 >> $PROJECT_HOME/bin/ruscello/input/auc/eusage.txt
	ls -l $PROJECT_HOME/bin/ruscello/input/auc
;;

"autoCor")
	echo "running AutoCorrelation Spark job"
	CLASS_NAME=org.ruscello.explore.AutoCorrelation
	INPUT=file:///Users/pranab/Projects/bin/ruscello/input/auc/eusage.txt
	OUTPUT=file:///Users/pranab/Projects/bin/ruscello/output/auc
	rm -rf ./output/auc
	$SPARK_HOME/bin/spark-submit --class $CLASS_NAME   \
	--conf spark.ui.killEnabled=true --master $MASTER $RUSCELLO_JAR_NAME  $INPUT $OUTPUT cyd.conf
;;

*) 
	echo "unknown operation $1"
	;;

esac