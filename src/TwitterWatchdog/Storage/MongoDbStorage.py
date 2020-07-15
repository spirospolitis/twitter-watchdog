import logging

from .. import Globals as globals
from . import AbstractStorage

"""
    File: MongoDbStorage.py
    Date: 08/2019
    Author: Spiros Politis
    Python: 3.6
"""

"""
    A MongoDB connector.
"""


class MongoDbStorage(AbstractStorage.AbstractStorage):
    """
        Constructor
    """
    def __init__(self):
        AbstractStorage.AbstractStorage.__init__(self)

        self.__client = None
        self.__db = None
        self.__collection = None

    """
        Connects to MongoDB instance.

        :param host: MongoDB host.
        :param port: MongoDB port.
        :param username: MongoDB username.
        :param password: MongoDB password.
        :param auth_source: MongoDB authentication database.
        :param auth_mechanism: MongoDB authentication mechanism.
    """
    def connect(self, host: str, port: int, username: str, password: str, auth_source: str = 'admin', auth_mechanism: str = 'SCRAM-SHA-256'):
        from pymongo import MongoClient
        
        self.__client = MongoClient(
            host,
            username=username,
            password=password,
            authSource=auth_source,
            authMechanism=auth_mechanism
        )

        if globals.__DEBUG__:
            logging.debug(
                "MongoDbStorage:connect():host:{}:port:{}:username:{}:password:{}:auth_source:{}:auth_mechanism:{}"
                .format(host, port, username, password, auth_source, auth_mechanism)
            )
            logging.debug("MongoDbStorage:connect():client:{}".format(self.__client))

    """
        Disconnects the Mongo client from the DB.
    """
    def disconnect(self):
        if globals.__DEBUG__:
            logging.debug("MongoDbStorage:disconnect()")

        self.__client.disconnect()

    """
        Inserts one document in the specified collection.

        :param item: the JSON object representing a Tweet.

        :returns: Mongo ObjectId (key).
    """
    def insert_one(self, item: {}):
        if globals.__DEBUG__:
            logging.debug("MongoDbStorage:insert_one():item:{}".format(item))

        return self.__collection.insert_one(item).inserted_id

    """
        Properties
    """
    @property
    def client(self):
        return self.__client

    @property
    def db(self):
        return self.__db

    @db.setter
    def db(self, db: str):
        self.__db = self.__client[db]

    @property
    def collection(self):
        return self.__collection

    @collection.setter
    def collection(self, collection: str):
        self.__collection = self.__db[collection]
