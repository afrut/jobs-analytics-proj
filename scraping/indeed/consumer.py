#python consumer.py --config_file config.ini --reset
#ss consumer.py --config_file config.ini --reset
#watchFiles -Command ss -FileFiter
# A script that subscribes to a kafka topic and retrieves the HTML list for a
# job listing.
from argparse import ArgumentParser, FileType
from configparser import ConfigParser
from confluent_kafka import Consumer, OFFSET_BEGINNING
import re
from pyspark.sql import SparkSession
from delta import *
import random
from pyspark.sql.utils import AnalysisException
import time
from selenium import webdriver
from pyspark.sql.streaming import StreamingQuery
import datetime
from time import sleep
from pyspark.sql.functions import regexp_extract, udf
from pyspark.sql.types import StringType

from scrape import getSoup

if __name__ == '__main__':
    # Parse Kafka configuration file to get bootstrap server
    parser = ArgumentParser()
    parser.add_argument('--config_file', type = FileType('r'))
    parser.add_argument('--reset', action = 'store_true')
    args = parser.parse_args()
    config_parser = ConfigParser()
    config_parser.read_file(args.config_file)
    config = dict(config_parser['default'])
    config.update(config_parser['consumer'])

    # Create an entry point for spark
    spark = SparkSession.builder.appName("Test").getOrCreate()

    # Function to terminate query when no new data has arrived in timeout seconds
    def timeoutNewData(query: StreamingQuery, timeout: int):
        newData = datetime.datetime.now()
        while True:
            sleep(1)
            dct = query.lastProgress
            if dct is not None:
                if dct["numInputRows"] != 0:
                    newData = datetime.datetime.now()
                if (datetime.datetime.now() - newData).total_seconds() >= timeout:
                    break

    # User-defined function to get job posting HTML
    def getSoupDriver(url: str):
        with webdriver.Chrome() as driver:
            return getSoup(url, driver).prettify()

    # Use kafka as source for Spark Structured Streaming
    streaming = spark.readStream.format("kafka")\
        .option("kafka.bootstrap.servers", config["bootstrap.servers"])\
        .option("subscribe", "jobs")\
        .option("maxOffsetsPerTrigger", 1)\
        .option("startingOffsets","earliest")\
        .load()

    # Check if Delta Lake already exists to prevent overwrite
    deltaPath = "D:\\deltalakes\\jobs\\"
    checkPointLocation = "D:\\deltalakes\\jobsCheckpoint\\"
    write = False
    try:
        if spark.read.format("delta")\
            .option("path", deltaPath).load().count() != 0:
            write = True
    except AnalysisException:
        write = True

    if write:
        colNames = ["JobTitle", "Company", "Location", "Url", "FingerPrint"]
        getSoupUDF = udf(lambda x: getSoupDriver(x), StringType())
        csvValue = streaming.select(streaming["value"].cast("string"))
        pattern = r'"(.+?)","(.+?)","(.+?)","(.+?)","(.+?)"'
        cols = [regexp_extract(csvValue["value"], pattern, 1).alias("JobTitle")
            ,regexp_extract(csvValue["value"], pattern, 2).alias("Company")
            ,regexp_extract(csvValue["value"], pattern, 3).alias("Location")
            ,regexp_extract(csvValue["value"], pattern, 4).alias("Url")
            ,regexp_extract(csvValue["value"], pattern, 5).alias("FingerPrint")]
        separated = csvValue.select(*cols)
        query = separated.withColumn("Posting", getSoupUDF(separated["Url"]))\
            .writeStream.format("delta")\
            .outputMode("append")\
            .option("path", deltaPath)\
            .option("checkPointLocation", checkPointLocation)\
            .start()
            # .writeStream.format("console")\
            # .outputMode("append")\
            # .start()
        print(f"Writing data to Delta Lake at {deltaPath}")

        # Terminate streaming query after 3 seconds of not receiving new data
        timeoutNewData(query, 3)
        query.stop()
        query.awaitTermination()
        print(f"Finished writing to Delta Lake at {deltaPath}")
    else:
        print(f"Delta lake at {deltaPath} already exists")