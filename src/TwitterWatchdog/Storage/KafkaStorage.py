import logging
import json

import confluent_kafka as kafka

from .. import Globals as globals
from . import AbstractStorage

"""
    File: KafkaStorage.py
    Date: 08/2019
    Author: Spiros Politis
    Python: 3.6
"""

"""
    A Kafka connector.
"""


class KafkaStorage(AbstractStorage.AbstractStorage):
    """
        Constructor
    """
    def __init__(self):
        AbstractStorage.AbstractStorage.__init__(self)

        self.__producer = None
        self.__topic = None

    """
        Callback method for Kafka delivery reports. 
        
        Called once for each message produced to indicate delivery result.
        Triggered by poll() or flush().
            
        :param error: an error object, not None when an error has occurred.
        :param message: callback message.
    """
    def __on_delivery(self, error: kafka.KafkaError, message: kafka.Message):
        if error is not None:
            logging.error("KafkaStorage:__on_delivery():error:{}".format(error))
        else:
            if globals.__DEBUG__:
                logging.debug("KafkaStorage:__on_delivery():topic:{}:partition:{}:offset{}".format(message.topic(), message.partition(), message.offset()))

    """
        Callback method for Kafka generic stats reports.
            
        :param error: an error object, not None when an error has occurred.
        :param message: callback message.
    """
    def __on_stats(self, stats: json):
        if globals.__DEBUG__:
            logging.debug("KafkaStorage:__on_stats():{} [{}] [{}]".format(json.loads(stats)))

    """
        Connects to a Kafka endpoint and topic.
        
        :param endpoint: Kafka endpoint specification dictionary.
        :param topic: the Kafka topic to which to connect.
    """
    def connect(self, endpoint: {}, topic: str):
        from confluent_kafka import Producer

        if globals.__DEBUG__:
            logging.debug("KafkaStorage:connect():endpoint:{}:topic:{}".format(endpoint, topic))

        self.__producer = Producer(endpoint)
        self.__topic = topic

        if globals.__DEBUG__:
            logging.debug("KafkaStorage:connect():producer:{}".format(self.__producer))

    """
        Disconnects from the currently connected Kafka endpoint.
    """
    def disconnect(self):
        if globals.__DEBUG__:
            logging.debug("KafkaStorage:disconnect()")

        self.__producer.flush()

    """
        Pushes a Tweet to a Kafka topic.
        
        :param item: the JSON object representing a Tweet.
    """
    def insert_one(self, item: {}):
        import confluent_kafka as kafka

        if globals.__DEBUG__:
            logging.debug("KafkaStorage:insert_one():item:{}".format(item))

        # Trigger any available delivery report callbacks from previous produce() calls
        self.__producer.poll(0)

        # Asynchronously produce a message, the delivery report callback will be triggered from poll() above,
        # or flush() below, when the message has been successfully delivered or failed permanently.
        try:
            self.__producer.produce(self.__topic, key=bytes(str(item["tweet"]["id"]), encoding="utf8"), value=bytes(str(item), encoding="utf8"), callback=self.__on_delivery)
        except BufferError as buffer_error:
            logging.error("KafkaStorage:insert_one():BufferError:{}".format(buffer_error.str()))

        except kafka.KafkaException as kafka_exception:
            logging.error("KafkaStorage:insert_one():KafkaException:{}".format(kafka_exception.str()))

        # Wait for any outstanding messages to be delivered and delivery report callbacks to be triggered.
        self.__producer.flush()

    """
        Properties
    """
    @property
    def producer(self):
        return self.__producer

    @property
    def topic(self):
        return self.__topic
