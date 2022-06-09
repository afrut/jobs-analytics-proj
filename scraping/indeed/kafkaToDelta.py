#ss kafkaToDelta.py --config_file config.ini --reset
# A script that subscribes to a kafka topic and retrieves the HTML list for a
# job listing.
import datetime
import os
from shutil import rmtree
from argparse import ArgumentParser, FileType
from configparser import ConfigParser
from confluent_kafka import Consumer, OFFSET_BEGINNING
from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_extract, udf, lit
from pyspark.sql.types import StringType
from pyspark.sql.streaming import StreamingQuery
from pyspark.sql.utils import AnalysisException
from delta.tables import *
from selenium import webdriver
from time import sleep

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

    # Specify paths for the target delta lake and checkpoint location
    deltaPath = "D:\\deltalakes\\jobs\\"
    checkPointLocation = "D:\\deltalakes\\kafkaToDeltaCheckPoint\\"

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
    options = {
        "kafka.bootstrap.servers": config["bootstrap.servers"]
        ,"subscribe": "jobs"
        # ,"maxOffsetsPerTrigger": 10
    }
    if args.reset:
        options["startingOffsets"] = "earliest"
        if os.path.exists(checkPointLocation):
            rmtree(checkPointLocation)

    streaming = spark.readStream.format("kafka")\
        .options(**options)\
        .load()

    # Define a functions for upsert
    def upsert(microBatchOutputDF, batchId):
        dt = DeltaTable.forPath(deltaPath)
        dt.merge(microBatchOutputDF
            ,dt["FingerPrint"] == microBatchOutputDF["FingerPrint"])\
            .whenNotMatchedInsert(values = 
            {
                "JobTitle": microBatchOutputDF["JobTitle"]
                ,"Company": microBatchOutputDF["Company"]
                ,"Location": microBatchOutputDF["Location"]
                ,"Url": microBatchOutputDF["Url"]
                ,"FingerPrint": microBatchOutputDF["FingerPrint"]
            })

    # Check if Delta Lake already exists to prevent overwrite
    write = False
    try:
        if spark.read.format("delta")\
            .option("path", deltaPath).load().count() != 0:
            write = True
    except AnalysisException:
        write = True

    write = True
    if write:
        # Register user-defined function to get HTML of job posting
        getSoupUDF = udf(lambda x: getSoupDriver(x), StringType())

        # The value column contains the record of job posting metadata
        # Split this csv value into its components
        csvValue = streaming.select(streaming["value"].cast("string"))
        pattern = r'"(.+?)","(.+?)","(.+?)","(.+?)","(.+?)"'
        cols = [regexp_extract(csvValue["value"], pattern, 1).alias("JobTitle")
            ,regexp_extract(csvValue["value"], pattern, 2).alias("Company")
            ,regexp_extract(csvValue["value"], pattern, 3).alias("Location")
            ,regexp_extract(csvValue["value"], pattern, 4).alias("Url")
            ,regexp_extract(csvValue["value"], pattern, 5).alias("FingerPrint")]
        separated = csvValue.select(*cols)

        # Get job posting and write to Delta Lake
        #.withColumn("Posting", getSoupUDF(separated["Url"]))\
        query = separated.withColumn("Posting", lit(""))\
            .writeStream.format("delta")\
            .outputMode("append")\
            .option("path", deltaPath)\
            .option("checkPointLocation", checkPointLocation)\
            .start()
        print(f"Writing data to Delta Lake at {deltaPath}")

        # Terminate streaming query after 3 seconds of not receiving new data
        timeoutNewData(query, 3)
        query.stop()
        query.awaitTermination()
        print(f"Finished writing to Delta Lake at {deltaPath}")
    else:
        print(f"Delta lake at {deltaPath} already exists")