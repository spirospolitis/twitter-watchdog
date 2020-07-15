"""
    File: TestMongoDbStorage.py
    Date: 08/2019
    Author: Spiros Politis
    Python: 3.6
"""

"""
    Test suite for MongoDbStorage connector.
"""

import sys
import logging
import unittest

from TwitterWatchdog.Storage.MongoDbStorage import MongoDbStorage

# Set logging parameters
logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)


class TestMongoDbStorage(unittest.TestCase):

    """
        Unit test for insert_one.
    """
    def test_insert_one(self):
        import time

        logging.debug("TestMongoDbStorage:test_insert_one()")

        data = {
            "id": 1,
            "created_at": time.time(),
            "payload": "Test"
        }

        mongo_db_storage = MongoDbStorage()

        mongo_db_storage.connect(
            host="localhost",
            port=27017,
            username="twitter-watchdog",
            password="password",
            auth_source="admin",
            auth_mechanism="SCRAM-SHA-256"
        )

        mongo_db_storage.db = "twitter-watchdog"
        mongo_db_storage.collection = "tweets"

        mongo_db_storage.insert_one(data)


if __name__ == "__main__":
    unittest.main()
