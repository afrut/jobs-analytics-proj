#python getPosting.py --config_file config.ini --reset
#ss getPosting.py --config_file config.ini
#ss getPosting.py --config_file config.ini --reset
# A script that uses a Delta Lake as a streaming source and retrieves HTML
# postings for the associated record.

import os
from shutil import rmtree
from argparse import ArgumentParser, FileType
from configparser import ConfigParser
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, count, lit, sha2, col
from pyspark.sql.types import StringType
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from delta import DeltaTable
from time import sleep
from datetime import datetime
from random import random

from streamingFunc import timeoutNewData
from scrape import getSoup

if __name__ == '__main__':
    # Parse Kafka configuration file to get bootstrap server
    parser = ArgumentParser()
    parser.add_argument('--config_file', type = FileType('r'))
    parser.add_argument('--reset', action = 'store_true', help = "Read Kafka stream starting from earliest offset")
    args = parser.parse_args()
    config_parser = ConfigParser()
    config_parser.read_file(args.config_file)
    config = dict(config_parser['default'])
    config.update(config_parser['consumer'])
    
    # Create an entry point for spark
    conf = SparkConf()
    conf.set("spark.app.name", "Test")
    spark = SparkSession.builder.config(conf = conf).getOrCreate()
    deltaPath = "D:\\deltalakes\\jobs\\"
    checkPointLocation = "D:\\deltalakes\\getPostingCheckPoint"

    # Function to retrieve HTML of job posting
    def getSoupDriver(url: str):
        chromePath = "C:\\Program Files\\Google\\Chrome\\Application\\chrome.exe"
        chromeDriverPath = "D:\\scraping\\driver\\chromedriver.exe"
        options = Options()
        options.add_argument("--headless")
        options.binary_location = chromePath
        with webdriver.Chrome(executable_path = chromeDriverPath, chrome_options = options) as driver:
            now = datetime.now()
            print(f"{now} Getting posting")
            return getSoup(url, driver).prettify()
    getSoupUDF = udf(getSoupDriver, StringType())

    # Function to execute upsert on DeltaTable
    def upsert(microBatchOutputDF, batchId):
        deltaTable = DeltaTable.forPath(spark, deltaPath)
        deltaTable.alias("table")\
            .merge(microBatchOutputDF\
                .withColumn("Posting", getSoupUDF(microBatchOutputDF["value"]))\
                .withColumn("PostingFingerPrint", sha2(col("Posting"), 256))
                .alias("updates")
                ,"table.FingerPrint = updates.key")\
            .whenMatchedUpdate(set = {
                "Posting": "updates.Posting"
                ,"PostingFingerPrint": "updates.PostingFingerPrint"
            })\
            .execute()
        now = datetime.now()
        print(f"{now} Writing to Delta Lake")

    # Use Kafka topic as streaming source
    topic = "get-posting"
    options = {
        "kafka.bootstrap.servers": config["bootstrap.servers"]
        ,"subscribe": topic
        ,"maxOffsetsPerTrigger": 1
    }
    if args.reset:
        options["startingOffsets"] = "earliest"
    streaming = spark.readStream.format("kafka")\
        .options(**options)\
        .load()\
        .withColumn("key", col("key").cast("string"))\
        .withColumn("value", col("value").cast("string"))\
        .select("key", "value")

    # Use Delta Lake as sink
    query = streaming\
        .writeStream.format("delta")\
        .foreachBatch(upsert)\
        .outputMode("update")\
        .option("path", deltaPath)\
        .option("checkPointLocation", checkPointLocation)\
        .start()

    timeoutNewData(query, 3)
    query.stop()
    query.awaitTermination()

    spark.stop()