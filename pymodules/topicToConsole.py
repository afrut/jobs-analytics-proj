#python ..\..\pymodules\topicToConsole.py --group test --topic job-listing --reset
# A script that consumes a kafka topic and prints to console.
from argparse import ArgumentParser
from confluent_kafka import Consumer, OFFSET_BEGINNING
from datetime import datetime

if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument("-s", "--server", type = str, default = "[::1]:9092", help = "Server address for Kafka bootstrap.servers. Defaults to [::1]:9092.")
    parser.add_argument("-g", "--group", type = str, required = True, help = "Consumer group id")
    parser.add_argument("-r", "--reset", action = "store_true", help = "Resets offset and read topic from the beginning")
    parser.add_argument("-t", "--topic", type = str, default = "test", help = "Topic to consume from")
    args = parser.parse_args()

    # Consumer configuration and creation
    config = dict()
    config["bootstrap.servers"] = args.server
    config["group.id"] = args.group
    config["auto.offset.reset"] = "earliest"
    consumer = Consumer(config)

    # Call back to reset offset if necessary
    def reset_offset(consumer, partitions):
        if args.reset:
            for p in partitions:
                p.offset = OFFSET_BEGINNING
            consumer.assign(partitions)

    # Subscribe to a topic
    consumer.subscribe([args.topic], on_assign = reset_offset)

    # Poll for messages
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                print("Waiting...")
            elif msg.error():
                print(f"ERROR: {msg.error()}")
            else:
                now = datetime.now()
                key = msg.key()
                value = msg.value()
                print(f"{now} Consumed message from {args.topic}, key = {key}, value = {value.decode('utf-8')}")
    except KeyboardInterrupt:
        pass
    finally:
        print(consumer.position(consumer.assignment())[0].offset)
        consumer.close()