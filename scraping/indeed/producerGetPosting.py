#ss producerGetPosting.py --config_file config.ini --reset
# A script that writes jobs to a topic to have their posting acquired.
import datetime
import os
from shutil import rmtree
from argparse import ArgumentParser, FileType
from configparser import ConfigParser
from confluent_kafka import Consumer, OFFSET_BEGINNING
from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_extract, udf, lit, col
from pyspark.sql.types import StringType, StructType, StructField
from pyspark.sql.streaming import StreamingQuery
from pyspark.sql.utils import AnalysisException
from delta.tables import *
from selenium import webdriver
from time import sleep

from scrape import getSoup
from streamingFunc import timeoutNewData
from initTopic import initTopic

if __name__ == '__main__':
    # Parse Kafka configuration file to get bootstrap server
    parser = ArgumentParser()
    parser.add_argument('--config_file', type = FileType('r'))
    parser.add_argument('--reset', action = 'store_true', help = "Re-initialize get-posting topic")
    args = parser.parse_args()
    config_parser = ConfigParser()
    config_parser.read_file(args.config_file)
    config = dict(config_parser['default'])
    config.update(config_parser['consumer'])

    # Specify paths for the target delta lake and checkpoint location
    deltaPath = "D:\\deltalakes\\jobs\\"
    checkpointLocation = "D:\\deltalakes\\producerGetPostingCheckPoint\\"

    # Create an entry point for spark
    spark = SparkSession.builder.appName("Test").getOrCreate()

    # Re-initialize topic if requested
    topic = "get-posting"
    if args.reset:
        initTopic(topic)

    # Use Delta Lake as streaming source
    options = {
        "path": deltaPath
        ,"ignoreChanges": True
    }
    streaming = spark.readStream.format("delta")\
        .options(**options)\
        .load()\
        .filter(col("Posting").isNull())\
        .withColumn("key", col("FingerPrint"))\
        .withColumn("value", col("Url"))\
        .select("key", "value")

    query = streaming.writeStream.format("kafka")\
        .option("kafka.bootstrap.servers", config["bootstrap.servers"])\
        .option("topic", topic)\
        .option("checkpointLocation", checkpointLocation)\
        .start()
    print(f"Writing data to Kafka topic {topic}")

    # Terminate streaming query after 3 seconds of not receiving new data
    timeoutNewData(query, 3)
    query.stop()
    query.awaitTermination()
    print(f"Finished writing to Kafka topic {topic}")

    spark.stop()