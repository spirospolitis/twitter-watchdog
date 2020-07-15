import csv
import json
import logging

from .. import Globals as globals
from . import AbstractStorage

"""
    File: AbstractStorage.py
    Date: 08/2019
    Author: Spiros Politis
    Python: 3.6
"""

"""
    Implements a simple, local file system storage mechanism.
"""


class CSVFileStorage(AbstractStorage.AbstractStorage):
    """
        Constructor
    """
    def __init__(self):
        AbstractStorage.AbstractStorage.__init__(self)

        self.__file = None
        self.__csv_writer = None
        
    def __flatten_json(self, json_object: object, field_delimiter: str = "_"):
        val = {}
        
        for i in json_object.keys():
            if isinstance(json_object[i], dict):
                get = self.__flatten_json(json_object[i], field_delimiter)
                
                for j in get.keys():
                    val[i + field_delimiter + j] = get[j]
            else:
                val[i] = json_object[i]

        return val
    
    def __write_csv_header(self):
        self.__csv_writer.writerow(
            [   
                "tweet_id", 
                "tweet_created_at", 
                "tweet_text", 
                "tweet_source", 
                "tweet_in_reply_to_status_id", 
                "tweet_in_reply_to_user_id", 
                "tweet_in_reply_to_screen_name", 
                "tweet_quote_count", 
                "tweet_reply_count", 
                "tweet_retweet_count", 
                "tweet_favorite_count", 
                "tweet_entities_hashtags", 
                "tweet_entities_urls", 
                "tweet_entities_mentions", 
                "tweet_original", 
                "tweet_retweet", 
                "tweet_quoted_retweet", 
                "tweet_lang", 
                "user_id", 
                "user_name", 
                "user_screen_name", 
                "user_location", 
                "user_followers_count", 
                "user_friends_count", 
                "user_listed_count", 
                "user_favourites_count", 
                "user_statuses_count", 
                "user_created_at",  
                "user_following", 
            ]
        )
        

    """
        Opens a file.
        
        :param filename: file name.
        :param mode: read/write mode.
        :param encoding: file encoding.
    """
    def connect(self, filename: str, mode: str, encoding: str):
        if globals.__DEBUG__:
            logging.debug("CSVFileStorage:connect():filename:{}:mode:{}:encoding:{}".format(filename, mode, encoding))

        self.__file = open(filename, mode, encoding=encoding)
        self.__csv_writer = csv.writer(self.__file, delimiter = "\t")

        self.__write_csv_header()

        if globals.__DEBUG__:
            logging.debug("CSVFileStorage:connect():file:{}".format(self.__file))

    """
        Closes an open file.
    """
    def disconnect(self):
        if globals.__DEBUG__:
            logging.debug("CSVFileStorage:disconnect()")

        self.__file.flush()
        self.__file.close()

    """
        Writes one Tweet on file.
        
        :param item: the JSON object representing a Tweet.
    """
    def insert_one(self, item: {}):
        if globals.__DEBUG__:
            logging.debug("CSVFileStorage:insert_one:{}".format(item))

        field_delimiter = "_"

        # Flatten JSON object.
        item = self.__flatten_json(
            json_object = item, 
            field_delimiter = field_delimiter
        )

        item["tweet" + field_delimiter + "text"] = item["tweet" + field_delimiter + "text"].replace("\n", " ")
        item["tweet" + field_delimiter + "text"] = item["tweet" + field_delimiter + "text"].replace("\"", " ")

        self.__csv_writer.writerow(item.values())

        self.__file.flush()

    """
        Properties
    """
    @property
    def file(self):
        return self.__file
