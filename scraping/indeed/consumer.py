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

from scrape import getSoup

# TODO: ERROR: KafkaError{code=_MAX_POLL_EXCEEDED,val=-147,str="Application maximum poll interval (300000ms) exceeded by 406ms"}
# TODO: Instead of retrieving the HTML of a posting, read this from a Delta Lake to simulate accessing the HTML page.
# TODO: Use Spark Structured Streaming to read from a kafka topic. Use a UDF (getSavePosting) to retrieve the HTML of the job posting and save this to a Delta Lake.

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

    # Poll for new messages from Kafka and print them.
    ls = []
    colNames = ["JobTitle", "CompanyName", "Location", "Url", "Hash"]
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                # Initial message consumption may take up to
                # `session.timeout.ms` for the consumer group to
                # rebalance and start consuming
                if len(ls) > 0:
                    with webdriver.Chrome() as driver:
                        start = 0
                        N = len(ls)
                        while start + 5 < N:
                            getSavePosting(ls[start : start + 5])
                            start = start + 5
                        getSavePosting(ls[start:])
                    ls.clear()
                print("Waiting...")
            elif msg.error():
                print("ERROR: {0}".format(msg.error()))
            else:
                # Extract the (optional) key and value, and print.
                key = msg.key()
                if key is not None:
                    key = key.decode('utf-8')
                else:
                    key = ''
                text = msg.value().decode("utf-8")
                ls.append(re.findall(r'"(.*?)"', text))
                # print("Consumed event from topic {topic}: key = {key:12} value = {value:12}".format(
                #     topic=msg.topic(), key = key, value = text))
    except KeyboardInterrupt:
        pass
    finally:
        # Leave group and commit final offsets
        consumer.close()
        spark.stop()