# Check to see if a topic exists. If it does, delete and create it. If not, create it.
#exec(open("initTopic.py").read())
#python initTopic.py --server [::1]:9092 --config_file config.ini --topic jobs
from confluent_kafka.admin import AdminClient
from confluent_kafka.admin import NewTopic
from time import sleep
from argparse import ArgumentParser, FileType
from configparser import ConfigParser

def initTopic(topicName: str, server: str = "[::1]:9092", ac: AdminClient = None):
    # Create an admin client. Specify server.
    if ac is None:
        ac = AdminClient({"bootstrap.servers": server})

    # Check if topic already exists. Delete if it does.
    topics = ac.list_topics().topics
    if topicName in topics.keys():
        dctFuture = ac.delete_topics([topicName])
        future = dctFuture[topicName]
        future.result()

    # Create a topic. Keep trying until no errors.
    # Errors can come up due to topic deletion being incomplete.
    while True:
        try:
            dctFuture = ac.create_topics([NewTopic(topicName, 1)], operation_timeout = 60)
            future = dctFuture[topicName]
            future.result()     # Block until topic has been created
            break
        except Exception as e:
            if e.args[0].name() == "TOPIC_ALREADY_EXISTS":
                # Sleep for 1 second if topic still exists
                sleep(1)



if __name__ == "__main__":
    # Parse command-line arguments
    parser = ArgumentParser()
    parser.add_argument("--server", type = str)
    parser.add_argument("--config_file", type = FileType("r"))
    parser.add_argument("--topic", type = str)
    args = parser.parse_args()

    config_parser = ConfigParser()
    config_parser.read_file(args.config_file)
    config = dict(config_parser['default'])

    # Create an admin client. Specify server.
    ac = AdminClient({"bootstrap.servers": args.server})

    # Initialize a topic.
    initTopic(args.topic, args.server)

    # Check if topic was created
    topics = ac.list_topics().topics
    if args.topic in topics.keys():
        print(f"Topic {args.topic} initialized.")
