#!/bin/bash

# Set the environment variables for Spark and Hive
export SPARK_HOME=/opt/spark2
export HADOOP_HOME=/opt/hadoop
export HIVE_HOME=/opt/hive

# Run the Spark-SQL command to execute the Hive script
$SPARK_HOME/bin/spark-sql --master yarn --conf spark.sql.shuffle.partitions=5 -f /home/itversity/HiveInsertion.hql





