"""
    File: TwitterHelper.py
    Date: 08/2019
    Author: Spiros Politis
    Python: 3.6
"""

import argparse
import asyncio
from datetime import datetime
import logging
import pathlib
import os
import setproctitle
import signal
import sys

from TwitterWatchdog import TwitterWatchdog
from TwitterWatchdog import Globals as globals
from TwitterWatchdog.Config import FileConfigurator
from TwitterWatchdog.Util import GeoHelper, TwitterHelper
from TwitterWatchdog.Storage import CSVFileStorage, JSONFileStorage, MongoDbStorage, HadoopWebStorage, KafkaStorage

topics = []

storage = []

file_storage = None
hadoop_storage = None
kafka_storage = None
mongodb_storage = None

twitter_watchdog = None


"""
    Handle Ctrl-C.
"""


def sigint_handler(signal_received, frame):
    if file_storage is not None:
        file_storage.disconnect()

    if hadoop_storage is not None:
        hadoop_storage.disconnect()
    
    if kafka_storage is not None:
        kafka_storage.disconnect()
    
    if mongodb_storage is not None:
        mongodb_storage.disconnect()

    if twitter_watchdog is not None:
        twitter_watchdog.disconnect()

    sys.exit(0)


"""
    Creates a plain file storage endpoint.
    
    :param file_path: Directory in which to store files.
    
    :returns: Instance of FileStorage.
"""


def create_file_storage(format: str, file_path: str):
    import time
    import os
    import errno

    if globals.__DEBUG__:
        logging.debug("main:create_file_storage()")

    # Create dir if not exists
    try:
        os.makedirs(file_path)
    except OSError as os_error:
        if os_error.errno != errno.EEXIST:
            raise
    
    if format == "json":
        file_storage = JSONFileStorage.JSONFileStorage()

        file_storage.connect(
            filename=os.path.join(file_path, "{}.json".format(datetime.now().strftime("%Y%m%d_%H%M%S"))),
            mode="w",
            encoding="utf-8"
        )

    if format == "csv":
        file_storage = CSVFileStorage.CSVFileStorage()

        file_storage.connect(
            filename=os.path.join(file_path, "{}.csv".format(datetime.now().strftime("%Y%m%d_%H%M%S"))),
            mode="w",
            encoding="utf-8"
        )

    return file_storage


"""
    Creates a Hadoop storage endpoint.
"""


def create_hadoop_storage():
    if globals.__DEBUG__:
        logging.debug("main:create_hadoop_storage()")

    return None


"""
    Creates a Kafka storage endpoint.
    
    :param endpoint: Dictionary of configuration pearameters.
    :param topic: The Kafka topic to connect.
    
    :returns: Instance of KafkaStorage.
"""


def create_kafka_storage(endpoint: {}, topic: str):
    if globals.__DEBUG__:
        logging.debug("main:create_kafka_storage()")

    kafka_storage = KafkaStorage.KafkaStorage()

    kafka_storage.connect(
        endpoint=endpoint,
        topic=topic
    )

    return kafka_storage


"""
    Creates a Mongo DB storage endpoint.
    
    :param host: Mongo DB host.
    :param port: Mongo DB port.
    :param username: Mongo DB username.
    :param password: Mongo DB password.
    :param auth_source: Monog DB authentication source (e.g. admin). 
    :param db: Mongo DB database. 
    :param collection: Mongo DB collection.
    
    :returns: Instance of MongoDbStorage.
"""


def create_mongodb_storage(
        host: str,
        port: int,
        username: str,
        password: str,
        auth_source: str,
        auth_mechanism: str,
        db: str,
        collection: str
):
    if globals.__DEBUG__:
        logging.debug("main:create_mongodb_storage()")

    mongo_db_storage = MongoDbStorage.MongoDbStorage()

    mongo_db_storage.connect(
        host=host,
        port=port,
        username=username,
        password=password,
        auth_source=auth_source,
        auth_mechanism=auth_mechanism
    )

    mongo_db_storage.db = db
    mongo_db_storage.collection = collection

    return mongo_db_storage


"""
    Retrieve tweets.
    
    :param config: Configuration parameters.
    :param topics: Twitter topics to watch for. 
    :param storage: Storage specification.
    
    :returns: Instance of TwitterWatchdog.
"""


def get_tweets(config: {}, topics: [], storage: []):
    global twitter_watchdog

    if twitter_watchdog is not None:
        twitter_watchdog.disconnect()
        twitter_watchdog = None

    twitter_watchdog = TwitterWatchdog.TwitterWatchdog(
        consumer_key=config["twitter_authentication"]["consumer_key"],
        consumer_secret=config["twitter_authentication"]["consumer_secret"],
        access_token=config["twitter_authentication"]["access_token"],
        access_token_secret=config["twitter_authentication"]["access_token_secret"],
        storage=storage,
        wait_on_rate_limit=config["tweepy"]["wait_on_rate_limit"]
    )

    if topics != None:
        twitter_watchdog.connect(
            track=topics,
            languages=config["languages"],
            is_async=config["tweepy"]["async"]
        )
    else:
        twitter_watchdog.connect(
            track=None,
            languages=config["languages"],
            is_async=config["tweepy"]["async"]
        )

    return twitter_watchdog


"""
    Retrieves trending topics from Twitter.
    
    :param config: The configuration dictionary.
    
    :returns: List of trending Twitter topics, depending on the parameters provided.
"""


def get_trending_topics(config: {}):
    trend_topics = []

    twitter_helper = TwitterHelper.TwitterHelper(
        consumer_key=config["twitter_authentication"]["consumer_key"],
        consumer_secret=config["twitter_authentication"]["consumer_secret"],
        access_token=config["twitter_authentication"]["access_token"],
        access_token_secret=config["twitter_authentication"]["access_token_secret"]
    )
    
    try:
        # Try to get WOE_ID from the locality parameter specified.
        geo_helper = GeoHelper.GeoHelper()
        
        woe_id = geo_helper.get_woe_id(config["locality"])

        trends = twitter_helper.get_trends(woe_id = woe_id)
        
        for trend in trends[0]["trends"]:
            trend_topics.append(trend["name"])
    except Exception as error:
        # If getting WOE_ID fails, hardcode to Greece.
        trends = twitter_helper.get_trends(woe_id = 23424833)
        
        for trend in trends[0]["trends"]:
            trend_topics.append(trend["name"])

    return trend_topics


"""
    Configuration file processing.

    :param config: Parameters dictionary.
"""


def process_config(config: {}):
    globals.__DEBUG__ = config["debug"]
    globals.__VERBOSE__ = config["verbose"]

    # Set logging parameters
    if globals.__DEBUG__:
        logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)
    else:
        logging.basicConfig(stream=sys.stdout, level=logging.INFO)

    global topics
    if config["topics"] is not None:
        topics = config["topics"]

        if globals.__DEBUG__:
            logging.debug("main:process_config():Found topics to watch:{}".format(topics))
    else:
        if globals.__DEBUG__:
            logging.debug("main:process_config():No predefined topics found")

    if config["locality"] is not None and config["topics"] is None:
        try:
            topics = get_trending_topics(config)
        except Exception as error:
            logging.error("main:process_config():Error getting trending topics:{}".format(error))

            topics = []

        if globals.__DEBUG__:
            logging.debug("main:process_config():Retrieved trending topics for {}:{}".format(config["locality"], topics))

    # Clear storage endpoints
    global storage
    storage.clear()

    params_storage = config["storage"]
    for param_storage in params_storage:
        if param_storage["enabled"]:

            if globals.__DEBUG__:
                logging.debug("main:process_config():storage type:{}".format(param_storage["type"]))

            if param_storage["type"] == "file":
                param_file_storage_format = param_storage["options"]["format"]
                param_file_storage_file_path = param_storage["options"]["file_path"]

                if param_file_storage_format is None:
                    raise ValueError("File connector: parameter 'format' cannot be null")

                if param_file_storage_file_path is None:
                    raise ValueError("File connector: parameter 'file_path' cannot be null")

                global file_storage
                if file_storage is not None:
                    file_storage.disconnect()

                file_storage = create_file_storage(param_file_storage_format, param_file_storage_file_path)

                storage.append(file_storage)

            if param_storage["type"] == "hadoop":

                global hadoop_storage
                if hadoop_storage is not None:
                    hadoop_storage.disconnect()

                hadoop_storage = create_hadoop_storage()

                storage.append(hadoop_storage)

            if param_storage["type"] == "kafka":
                param_kafka_storage_endpoint = param_storage["options"]["endpoint"]
                param_kafka_storage_topic = param_storage["options"]["topic"]

                if param_kafka_storage_endpoint is None:
                    raise ValueError("Kafka connector: parameter 'endpoint' cannot be null")

                if param_kafka_storage_endpoint["bootstrap.servers"] is None:
                    raise ValueError("Kafka connector: parameter 'endpoint.bootstrap.servers' cannot be null")

                if param_kafka_storage_topic is None:
                    raise ValueError("Kafka connector: parameter 'topic' cannot be null")

                global kafka_storage
                if kafka_storage is not None:
                    kafka_storage.disconnect()

                kafka_storage = create_kafka_storage(
                    param_kafka_storage_endpoint,
                    param_kafka_storage_topic
                )

                storage.append(kafka_storage)

            if param_storage["type"] == "mongodb":
                param_mongodb_storage_host = param_storage["options"]["host"]
                param_mongodb_storage_port = param_storage["options"]["port"]
                param_mongodb_storage_username = param_storage["options"]["username"]
                param_mongodb_storage_password = param_storage["options"]["password"]
                param_mongodb_storage_auth_source = param_storage["options"]["auth_source"]
                param_mongodb_storage_auth_mechanism = param_storage["options"]["auth_mechanism"]
                param_mongodb_storage_db = param_storage["options"]["db"]
                param_mongodb_storage_collection = param_storage["options"]["collection"]

                if param_mongodb_storage_host is None:
                    raise ValueError("MongoDB connector: parameter 'host' cannot be null")

                if param_mongodb_storage_port is None:
                    raise ValueError("MongoDB connector: parameter 'port' cannot be null")

                if param_mongodb_storage_username is None:
                    raise ValueError("MongoDB connector: parameter 'username' cannot be null")

                if param_mongodb_storage_password is None:
                    raise ValueError("MongoDB connector: parameter 'password' cannot be null")

                if param_mongodb_storage_auth_source is None:
                    raise ValueError("MongoDB connector: parameter 'auth_source' cannot be null")

                if param_mongodb_storage_auth_mechanism is None:
                    raise ValueError("MongoDB connector: parameter 'auth_mechanism' cannot be null")

                if param_mongodb_storage_db is None:
                    raise ValueError("MongoDB connector: parameter 'db' cannot be null")

                if param_mongodb_storage_collection is None:
                    raise ValueError("MongoDB connector: parameter 'collection' cannot be null")

                global mongodb_storage
                if mongodb_storage is not None:
                    mongodb_storage.disconnect()


"""
    Async method for polling the configuration file for parameter changes.
    
    :param queue: A asyncio.Queue containing the configuration dictionary. 
"""


async def wait_on_config_queue(queue: asyncio.Queue):
    current_config = None

    while True:
        config = await queue.get()

        if current_config is None:
            current_config = config

            if globals.__DEBUG__:
                logging.debug("main:wait_on_config_queue():Initial configuration:config:{}".format(current_config))

            process_config(current_config)

            get_tweets(config, topics, storage)
        else:
            if current_config != config:
                current_config = config

                if globals.__DEBUG__:
                    logging.debug("main:wait_on_config_queue():Configuration changed:config:{}".format(current_config))

                process_config(current_config)

                get_tweets(config, topics, storage)

        await asyncio.sleep(20)


if __name__ == "__main__":

    argument_parser = argparse.ArgumentParser()
    argument_parser.add_argument("-p", action = "store", dest = "os_process_name", help = "OS process name")
    argument_parser.add_argument("-c", action = "store", dest = "config_file_path", help = "Path to config.json")
    args = argument_parser.parse_args()
    
    try:
        setproctitle.setproctitle(args.os_process_name)
    except Exception as error:
        logging.error(f"Missing process title, supplied value is {args.os_process_name}")

        sys.exit()

    try:
        config_path = os.path.normpath(os.path.join(pathlib.Path.cwd().parents[0], args.config_file_path))
    except Exception as error:
        logging.error(f"Missing config.json, supplied value is {config_path}")

        sys.exit()

    # Handler of SIGINT
    signal.signal(signal.SIGINT, sigint_handler)

    try:
        event_loop = asyncio.get_event_loop()

        config_queue = asyncio.Queue(maxsize=1)

        file_configurator = FileConfigurator.FileConfigurator()

        event_loop.run_until_complete(
            asyncio.gather(
                file_configurator.get_config(config_queue, 30, config_path),
                wait_on_config_queue(config_queue)
            )
        )
    finally:
        if globals.__DEBUG__:
            logging.debug("main:closing event loop")
