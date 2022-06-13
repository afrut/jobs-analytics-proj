#python getPosting.py --config_file config.ini --reset
#ss getPosting.py --config_file config.ini
#ss getPosting.py --config_file config.ini --reset
# A script that uses a Delta Lake as a streaming source and retrieves HTML
# postings for the associated record.

from argparse import ArgumentParser, FileType
from configparser import ConfigParser
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, count, lit, sha2, col
from pyspark.sql.types import StringType
from delta import DeltaTable
from time import sleep
from random import random

import warnings
from datetime import datetime
from selenium.webdriver.chrome.options import Options
from scrape import getSoup
from selenium import webdriver

from streamingFunc import timeoutNewData

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

    # Constants
    deltaPath = "D:\\deltalakes\\jobs\\"
    checkPointLocation = "D:\\deltalakes\\getPostingCheckPoint"
    
    # Create an entry point for spark
    conf = SparkConf()
    conf.set("spark.app.name", "Test")
    spark = SparkSession.builder.config(conf = conf).getOrCreate()

    # Function to retrieve HTML of job posting
    def getSoupDriver(url: str):
        warnings.filterwarnings("ignore", category = DeprecationWarning)
        chromePath = "C:\\Program Files\\Google\\Chrome\\Application\\chrome.exe"
        chromeDriverPath = "D:\\scraping\\driver\\chromedriver.exe"
        options = Options()
        options.add_argument("--headless")
        options.binary_location = chromePath
        with webdriver.Chrome(executable_path = chromeDriverPath, options = options) as driver:
            now = datetime.now()
            print(f"{now} Getting posting")
            return getSoup(url, driver).prettify()
    getSoupUDF = udf(getSoupDriver, StringType())

    # Function to execute upsert on DeltaTable
    def upsert(microBatchOutputDF, batchId):
        deltaTable = DeltaTable.forPath(spark, deltaPath)
        # Don't use withColumn here for the posting fingerprint as the UDF will
        # be executed twice.
        updates = microBatchOutputDF\
            .withColumn("Posting", getSoupUDF(microBatchOutputDF["value"]))
        deltaTable.alias("table")\
            .merge(updates.alias("updates")
                ,"table.FingerPrint = updates.key")\
            .whenMatchedUpdate(set = {
                "Posting": updates["Posting"]
                ,"PostingFingerPrint": sha2(updates["Posting"], 256)
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