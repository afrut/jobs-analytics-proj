# Functions used with Spark Structured Streaming
from pyspark.sql.streaming import StreamingQuery
from datetime import datetime
from time import sleep

# Function to terminate query when no new data has arrived in timeout seconds
def timeoutNewData(query: StreamingQuery, timeout: int):
    newData = datetime.now()
    while True:
        sleep(1)
        dct = query.lastProgress
        if dct is not None:
            if dct["numInputRows"] != 0:
                newData = datetime.now()
            if (datetime.now() - newData).total_seconds() >= timeout:
                break