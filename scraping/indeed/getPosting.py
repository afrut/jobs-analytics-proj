#python getPosting.py
#ss getPosting.py
# A script that uses a Delta Lake as a streaming source and retrieves HTML
# postings for the associated record.

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, count, lit
from pyspark.sql.types import StringType
from selenium import webdriver
from delta import DeltaTable

from streamingFunc import timeoutNewData
from scrape import getSoup

if __name__ == '__main__':
    # Create an entry point for spark
    spark = SparkSession.builder.appName("Test").getOrCreate()
    deltaPath = "D:\\deltalakes\\jobs\\"
    checkPointLocation = "D:\\deltalakes\\getPostingCheckPoint"

    # Function to retrieve HTML of job posting
    def getSoupDriver(url: str):
        with webdriver.Chrome() as driver:
            return getSoup(url, driver).prettify()
    getSoupUDF = udf(getSoupDriver, StringType())

    # Function to execute upsert on DeltaTable
    def upsert(microBatchOutputDF, batchId):
        deltaTable = DeltaTable.forPath(spark, deltaPath)
        deltaTable.alias("table")\
            .merge(microBatchOutputDF\
                .withColumn("Posting", getSoupUDF(microBatchOutputDF["Url"]))\
                .alias("updates")
                ,"table.FingerPrint = updates.FingerPrint")\
            .whenMatchedUpdate(set = {
                "Posting": "updates.Posting"
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

    # query = streaming.filter(streaming["Posting"].isNull()).select(count(lit(0))).writeStream.format("console")\
    #     .outputMode("complete")\
    #     .start()

    query = streaming\
        .filter(streaming["Posting"].isNull())\
        .writeStream.format("delta")\
        .foreachBatch(upsert)\
        .outputMode("update")\
        .option("path", deltaPath)\
        .option("checkPointLocation", checkPointLocation)\
        .start()
        # .withColumn("Posting", getSoupUDF(streaming["Url"]))\

    timeoutNewData(query, 3)
    query.stop()
    query.awaitTermination()

    spark.stop()