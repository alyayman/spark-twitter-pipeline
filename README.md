# Twitter Data Pipeline
This repository contains the source code and documentation for a data pipeline that collects, processes, and analyzes Twitter data using various technologies such as Python, Hive, Spark, and HDFS.

## Overview
The pipeline consists of several stages, each of which performs a specific task on the data:

*  __Data Source System__: A Python script that fetches the latest tweets from Twitter API and pushes them to a port for communication with other stages.
* __Data Collection System__: A streaming component that receives data from the port, parses it, gives it structure, and stores it in HDFS partitioned by time.
* __Landing Data Persistence__: A module that stores the raw data from HDFS in its base format as Parquet and creates a Hive table for it.
Landing To Raw ETL: A set of HiveQL scripts that extract dimensions from the landing data and store them as tables partitioned by time.
* __Raw To Processed ETL__: A SparkSQL application that aggregates the dimensions to create fact tables with metrics such as tweet counts and favorites, stored in HDFS with the suffix "-processed".
* __Shell Script Coordinator__: A script that coordinates the execution of all pipeline components and ensures the SparkStream is running.
Requirements
To use this pipeline, you need to have the following software installed:

* Python 3.x
* Apache Hive
* Apache Spark
* Hadoop Distributed File System (HDFS)

You also need to obtain a Twitter API key and provide it in the Python script for data retrieval.

## Usage
To use the pipeline, follow these steps:

* Clone this repository to your local machine.
* Set up the required software and configurations as described in the documentation.
* Run the shell script coordinator to start the pipeline components.
* Monitor the pipeline logs and output files to ensure data is flowing correctly.
* Customize the pipeline components to suit your specific use case.


