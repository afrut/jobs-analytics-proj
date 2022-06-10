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
from pyspark.sql.types import StringType, StructType, StructField
from pyspark.sql.streaming import StreamingQuery
from pyspark.sql.utils import AnalysisException
from delta.tables import *
from selenium import webdriver
from time import sleep

from scrape import getSoup
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

    # Specify paths for the target delta lake and checkpoint location
    deltaPath = "D:\\deltalakes\\jobs\\"
    checkPointLocation = "D:\\deltalakes\\kafkaToDeltaCheckPoint\\"

    # Create an entry point for spark
    spark = SparkSession.builder.appName("Test").getOrCreate()

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

    # Define a function for upsert
    def upsert(microBatchOutputDF, batchId):
        dt = DeltaTable.forPath(spark, deltaPath)
        dt.alias("table").merge(microBatchOutputDF.dropDuplicates(["FingerPrint"]).alias("updates")
            ,"table.FingerPrint = updates.FingerPrint")\
            .whenNotMatchedInsert(values = {
                "JobTitle": "updates.JobTitle"
                ,"Company": "updates.Company"
                ,"Location": "updates.Location"
                ,"Url": "updates.Url"
                ,"FingerPrint": "updates.FingerPrint"
                ,"Posting": lit(None)
            }).execute()

    # Initialize DeltaTable if it doesn't exist
    try:
        dt = DeltaTable.forPath(spark, deltaPath)
    except AnalysisException as e:
        if os.path.exists(deltaPath):
            print(f"Path {deltaPath} exists")
            raise
        else:
            schema = StructType([
                StructField("JobTitle", StringType(), False)
                ,StructField("Company", StringType(), False)
                ,StructField("Location", StringType(), False)
                ,StructField("Url", StringType(), False)
                ,StructField("FingerPrint", StringType(), False)
                ,StructField("Posting", StringType(), True)
            ])
            df = spark.createDataFrame(data = [], schema = schema)
            df.write.format("delta")\
                .option("path", deltaPath)\
                .save()

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
    query = separated\
        .writeStream.format("delta")\
        .outputMode("update")\
        .option("path", deltaPath)\
        .option("checkPointLocation", checkPointLocation)\
        .foreachBatch(upsert)\
        .start()
    print(f"Writing data to Delta Lake at {deltaPath}")

    # Terminate streaming query after 3 seconds of not receiving new data
    timeoutNewData(query, 3)
    query.stop()
    query.awaitTermination()
    print(f"Finished writing to Delta Lake at {deltaPath}")

    spark.stop()