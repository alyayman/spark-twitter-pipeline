import pyspark # run after findspark.init() if you need it
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
spark = SparkSession.builder.appName("factTable")\
.config("spark.sql.warehouse.dir", "/user/hive/warehouse").config("spark.executor.instances", "5").master("yarn").enableHiveSupport().getOrCreate()
spark.sql("use warehouse_twitter")
spark.conf.set("spark.sql.shuffle.partitions",10)
user_dim=spark.sql("select * from user_dim_raw")
tweet_dim=spark.sql("select * from tweet_dim_raw")
user_dim=user_dim.drop("year","month","day","hour")

fact_table=tweet_dim.join(broadcast(user_dim),tweet_dim.author_id==user_dim.user_id).groupBy("year","month","day","hour","score").\
agg(expr("count(tweet_id) as numberOfTweets"),expr("round(avg(like_count),2) as avgLikes"),expr("round(avg(retweet_count),2) as avgRetweetCount"),expr("round(avg(impression_count),2) as avg_impression"))
fact_table.write.format("hive").mode("overwrite").saveAsTable("warehouse_twitter.fact_table_processed")
