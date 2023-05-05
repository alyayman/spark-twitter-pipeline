#!/bin/bash
# Set the environment variables for Spark and Hive
export SPARK_HOME=/opt/spark2
export HADOOP_HOME=/opt/hadoop
export HIVE_HOME=/opt/hive
# Run the Spark-SQL command to execute the Hive script
$SPARK_HOME/bin/spark-submit --name "Fact" --deploy-mode cluster /home/itversity/fact.py