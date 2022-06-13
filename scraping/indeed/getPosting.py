#python getPosting.py
#ss getPosting.py
# A script that uses a Delta Lake as a streaming source and retrieves HTML
# postings for the associated record.

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
        # options.add_argument("--window-size=1920,1080")
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
                .withColumn("Posting", getSoupUDF(microBatchOutputDF["Url"]))\
                .withColumn("PostingFingerPrint", sha2(col("Posting"), 256))
                .alias("updates")
                ,"table.FingerPrint = updates.FingerPrint")\
            .whenMatchedUpdate(set = {
                "Posting": "updates.Posting"
                ,"PostingFingerPrint": "updates.PostingFingerPrint"
            })\
            .execute()

    options = {
        "path": deltaPath
        ,"maxFilesPerTrigger": 2
        ,"ignoreChanges": True      # Needed to handle updates in the source DeltaTable
    }
    streaming = spark.readStream.format("delta")\
        .options(**options)\
        .load()

    query = streaming\
        .filter(streaming["Posting"].isNull())\
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