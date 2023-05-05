# Import your dependecies
import pyspark # run after findspark.init() if you need it
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from textblob import TextBlob
from pyspark.sql.functions import udf
from pyspark.sql.types import FloatType
# from pyspark.sql.functions import col, split


from pyspark.sql.functions import udf
from pyspark.sql.types import StringType





# Start up your pyspark session as always
# Don't run this more than once
spark = SparkSession.builder.appName("TwitterStream1")\
.config("spark.sql.warehouse.dir","/user/hive/warehouse").config("spark.executor.instances","2").master("local[*]").getOrCreate()
spark.conf.set("spark.sql.shuffle.partitions",4)

def sentiment_analysis(text):
    blob = TextBlob(text)
    sentiment = blob.sentiment.polarity
    return sentiment

sentiment_udf = udf(sentiment_analysis, FloatType())
spark.conf.set("hive.exec.dynamic.partition", "true")
spark.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")

# read the tweet data from socket
tweet_df = spark \
    .readStream \
    .format("socket") \
    .option("host", "127.0.0.1") \
    .option("port", 7777) \
    .load()

spark.conf.set("spark.sql.streaming.checkpointLocation", "checkpoints/")
tweet_df_string = tweet_df.selectExpr("value as rows")

tweets_tab =tweet_df_string.\
withColumn("Created_at",get_json_object(col("rows"), "$.created_at")).\
withColumn("text",get_json_object(col("rows"), "$.text")).\
withColumn("Created_at", from_utc_timestamp("Created_at", "UTC")).\
withColumn("year",year("created_at")).withColumn("month",month("created_at")).withColumn("day",dayofmonth("created_at")).\
withColumn("hour",hour("created_at")).\
withColumn("retweet_count",get_json_object(col("rows"), "$.public_metrics.retweet_count")).\
withColumn("like_count",get_json_object(col("rows"), "$.public_metrics.like_count")).\
withColumn("impression_count",get_json_object(col("rows"), "$.public_metrics.impression_count"))


tweets_tab=tweets_tab.withColumn("retweet_count",get_json_object(col("rows"), "$.public_metrics.retweet_count")).\
withColumn("like_count",get_json_object(col("rows"), "$.public_metrics.like_count")).\
withColumn("impression_count",get_json_object(col("rows"), "$.public_metrics.impression_count")).\
withColumn("tweet_id",get_json_object(col("rows"), "$.id")).\
withColumn("author_id",get_json_object(col("rows"), "$.author_id")).\
withColumn("referenced_tweets_type",get_json_object(col("rows"), "$.referenced_tweets[0].type")).\
withColumn("referenced_tweets_id",get_json_object(col("rows"), "$.referenced_tweets[0].id")).\
withColumn("user_followers_count",get_json_object(col("rows"), "$.user.public_metrics.followers_count")).\
withColumn("user_name",get_json_object(col("rows"), "$.user.name")).\
withColumn("user_username",get_json_object(col("rows"), "$.user.username")).\
withColumn("user_location",get_json_object(col("rows"), "$.user.location")).\
withColumn("user_verified",get_json_object(col("rows"), "$.user.verified")).\
withColumnRenamed("rows","rows_data").\
withColumn("Score",sentiment_udf(col("text")))





tweets_tab=tweets_tab.withColumn("Score", when(tweets_tab["Score"] < 0, "negative")
                               .when(tweets_tab["score"] < 0.5 , "natural")
                               .otherwise("positive"))
tweets_tab=tweets_tab.coalesce(1)
writeTweet = tweets_tab.writeStream. \
    outputMode("append"). \
    format("parquet") \
    .partitionBy("year","month","day","hour") \
    .option("path", "/user/twitter-landing-data") \
    .option("mode","overwrite").\
    start()

    
writeTweet.awaitTermination()

