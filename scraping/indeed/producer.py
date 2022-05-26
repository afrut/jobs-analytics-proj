#exec(open(".\\scraping\\indeed\\producer.py").read())
#python producer.py --config_file config.ini
from argparse import ArgumentParser, FileType
from configparser import ConfigParser
from confluent_kafka import Producer
from time import sleep
from initTopic import initTopic
from datetime import datetime
import os

if __name__ == "__main__":
    # Parse the command line.
    parser = ArgumentParser()
    parser.add_argument('--config_file', type=FileType('r'))
    args = parser.parse_args()

    # Parse the configuration.
    # See https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
    config_parser = ConfigParser()
    config_parser.read_file(args.config_file)
    config = dict(config_parser['default'])

    # Create Producer instance
    producer = Producer(config)

    # Optional per-message delivery callback (triggered by poll() or flush())
    # when a message has been successfully delivered or permanently
    # failed delivery (after retries).
    def delivery_callback(err, msg):
        if err:
            print('ERROR: Message failed delivery: {}'.format(err))
        else:
            isent = int.from_bytes(msg.value(), "little")
            print(f"    {datetime.now()}: Sent {isent}")

    # Initialize topic
    topic = "jobs"
    initTopic(topic)

    # Open csv files and produce each line
    dirpath = ".\\csv\\"
    for filename in os.listdir(".\\csv\\"):
        filepath = dirpath + filename
        if filename.endswith(".csv") and os.path.isfile(filepath):
            with open(filepath, "rt") as file:
                lines = file.readlines()
                for line in lines:
                    producer.produce(topic, line, callback = delivery_callback)
                    producer.poll(10000)
                    producer.flush()