#python consumer.py --config_file config.ini --reset
#ss consumer.py --config_file config.ini --reset
# A script that subscribes to a kafka topic and retrieves the HTML list for a
# job listing.
import sys
from argparse import ArgumentParser, FileType
from configparser import ConfigParser
from confluent_kafka import Consumer, OFFSET_BEGINNING
import re
from pyspark.sql import SparkSession
from delta import *
import random
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
import time
import json

from scrape import getSoup

# TODO: Instead of retrieving the HTML of a posting, read this from a Delta Lake to simulate accessing the HTML page.
# TODO: Use a UDF (getSavePosting) to retrieve the HTML of the job posting and save this to a Delta Lake.

if __name__ == '__main__':
    # Parse the command line.
    parser = ArgumentParser()
    parser.add_argument('--config_file', type = FileType('r'))
    parser.add_argument('--reset', action = 'store_true')
    args = parser.parse_args()

    # Parse the configuration.
    # See https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
    config_parser = ConfigParser()
    config_parser.read_file(args.config_file)
    config = dict(config_parser['default'])
    config.update(config_parser['consumer'])

    # Create Consumer instance
    consumer = Consumer(config)

    # Set up a callback to handle the '--reset' flag.
    def reset_offset(consumer, partitions):
        if args.reset:
            for p in partitions:
                p.offset = OFFSET_BEGINNING
            consumer.assign(partitions)

    # Subscribe to topic
    topics = ["jobs"]
    consumer.subscribe(topics, on_assign = reset_offset)

    # Create an entry point for spark
    spark = SparkSession.builder.appName("Test") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()
    deltaPath = "D:\\deltalakes\\jobs\\"

    # Given a list of url's 
    def getSavePosting(ls: list):
        for entry in ls:
            url = entry[-2]
            soup = getSoup(url, driver)
            entry.append(soup.prettify())
            ts = random.randint(1,10) + random.random()
            print(f"Retrieved job posting for {entry[0]} --- {entry[1]} --- {entry[2]}. Sleeping for {ts:.2f}s")
            time.sleep(ts)
        df = spark.sparkContext.parallelize(ls).toDF(colNames)
        df.write.format("delta").mode("append").save(deltaPath)

    # Use kafka as source for Spark Structured Streaming
    offsets = {"jobs": {"0": 1335}}
    streaming = spark.readStream.format("kafka")\
        .option("kafka.bootstrap.servers", config["bootstrap.servers"])\
        .option("subscribe", "jobs")\
        .option("maxOffsetsPerTrigger", 1)\
        .option("startingOffsets", json.dumps(offsets))\
        .load()

    # Apply some transformations on the stream
    # nothing

    # Specify in-memory table as sink and start the stream in the background
    query = streaming.writeStream.queryName("records")\
        .format("memory").outputMode("append")\
        .start()

    while query.status["isTriggerActive"]:
        print("{0} - {1}".format(query.status, spark.sql("SELECT * FROM records").count()))
        time.sleep(1)
    
    # Prevent driver process from exiting while query is still running
    query.awaitTermination()