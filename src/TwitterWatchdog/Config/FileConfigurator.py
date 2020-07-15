"""
    File: FileConfigurator.py
    Date: 08/2019
    Author: Spiros Politis
    Python: 3.6
"""

import logging
import asyncio
import aiofiles
import json

from .. import Globals as globals


class FileConfigurator:
    """
        Constructor
    """
    def __init__(self):
        self.__filename = None
        self.__interval = None
        self.__queue = None

    """
        Retrieves application configuration file asynchronously.
        
        :param filename: Configuration file name.
        :param interval: File polling interval.
        :param callback: asyncio.coroutine to call when configuration file contents have been retrieved.
    """
    async def get_config(self, queue: asyncio.Queue, interval: int, filename: str):
        self.__filename = filename
        self.__interval = interval
        self.__queue = queue

        if globals.__DEBUG__:
            logging.debug("FileConfigurator:get_config():queue:{}:interval:{}:filename:{}".format(self.__queue, self.__interval, self.__filename))

        while True:
            async with aiofiles.open(self.__filename, mode='r') as file:
                config = json.loads(await file.read())

                if globals.__DEBUG__:
                    logging.debug("FileConfigurator:get_config():config:{}".format(config))

                await self.__queue.put(config)

            await asyncio.sleep(self.__interval)

    """
        Properties
    """
    @property
    def filename(self):
        return self.__filename

    @property
    def interval(self):
        return self.__interval

    @property
    def queue(self):
        return self.__queue
