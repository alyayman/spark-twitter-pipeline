create database IF NOT EXISTS warehouse_Twitter;
set hive.exec.dynamic.partition.mode=nonstrict;

CREATE EXTERNAL TABLE IF NOT EXISTS warehouse_Twitter.tweets (
  rows_data string,
  Created_at timestamp,
  text string,
  retweet_count string,
  like_count string,
  impression_count string,
  tweet_id string,
  author_id string,
  referenced_tweets_type string,
  referenced_tweets_id string,
  user_followers_count string,
  user_name string,
  user_username string,
  user_location string,
  user_verified string,
  Score STRING
)
PARTITIONED BY (year int, month int, day int, hour int)
STORED AS PARQUET
LOCATION '/user/twitter-landing-data';

CREATE TABLE IF NOT EXISTS warehouse_Twitter.user_dim_raw (
  user_id STRING,
  user_name STRING,
  user_username STRING,
  user_location STRING,
  user_verified STRING,
  user_followers_count INT
)
PARTITIONED BY (year int, month int, day int, hour int)
STORED AS PARQUET
LOCATION '/user/hive/twitter-raw-data/user';

CREATE TABLE IF NOT EXISTS warehouse_Twitter.tweet_dim_raw (
  tweet_id STRING ,
  text STRING,
  retweet_count INT,
  like_count INT,
  impression_count INT,
  referenced_tweets_type STRING,
  referenced_tweets_id STRING,
  author_id STRING,
  score STRING
)
PARTITIONED BY (year int, month int, day int, hour int)
STORED AS PARQUET
LOCATION '/user/hive/twitter-raw-data/tweet';

set hive.exec.dynamic.partition.mode=nonstrict;
msck repair table warehouse_twitter.tweets;

INSERT OVERWRITE TABLE warehouse_twitter.user_dim_raw PARTITION (year, month, day, hour)
SELECT DISTINCT
  author_id AS user_id,
  user_name,
  user_username,
  user_location,
  user_verified,
  user_followers_count,
  Year,
  Month,
  Day,
  Hour
FROM warehouse_twitter.tweets;

INSERT OVERWRITE TABLE warehouse_twitter.tweet_dim_raw PARTITION (year, month, day, hour)
SELECT DISTINCT
  tweet_id,
  text,
  retweet_count,
  like_count,
  impression_count,
  referenced_tweets_type,
  referenced_tweets_id,
  author_id,
  score,
  Year,
  Month,
  Day,
  Hour
FROM warehouse_twitter.tweets;


