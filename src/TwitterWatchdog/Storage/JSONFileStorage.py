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


class JSONFileStorage(AbstractStorage.AbstractStorage):
    """
        Constructor
    """
    def __init__(self):
        AbstractStorage.AbstractStorage.__init__(self)

        self.__file = None

    """
        Opens a file.
        
        :param filename: file name.
        :param mode: read/write mode.
        :param encoding: file encoding.
    """
    def connect(self, filename: str, mode: str, encoding: str):
        if globals.__DEBUG__:
            logging.debug("JSONFileStorage:connect():filename:{}:mode:{}:encoding:{}".format(filename, mode, encoding))

        self.__file = open(filename, mode, encoding=encoding)

        if globals.__DEBUG__:
            logging.debug("JSONFileStorage:connect():file:{}".format(self.__file))

    """
        Closes an open file.
    """
    def disconnect(self):
        if globals.__DEBUG__:
            logging.debug("JSONFileStorage:disconnect()")

        self.__file.flush()
        self.__file.close()

    """
        Writes one Tweet on file.
        
        :param item: the JSON object representing a Tweet.
    """
    def insert_one(self, item: {}):
        if globals.__DEBUG__:
            logging.debug("JSONFileStorage:insert_one:{}".format(item))

        json.dump(item, self.__file, ensure_ascii=False, indent=4)
        
        self.__file.flush()

    """
        Properties
    """
    @property
    def file(self):
        return self.__file
