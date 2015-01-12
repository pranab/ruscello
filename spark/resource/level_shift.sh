#!/bin/bash

PROJECT_HOME=/Users/pranab/Projects
JAR_NAME=$PROJECT_HOME/ruscello/spark/target/scala-2.10/ruscello-spark_2.10-1.0.jar
CLASS_NAME=org.ruscello.similarity.LevelSimilarity
MASTER=spark://Pranab-Ghoshs-MacBook-Pro.local:7077
export SPARK_CLASSPATH=$PROJECT_HOME/hoidla/target/hoidla-1.0.jar

$SPARK_HOME/bin/spark-submit --class $CLASS_NAME --conf spark.ui.killEnabled=true $JAR_NAME $MASTER level_shift.properties
