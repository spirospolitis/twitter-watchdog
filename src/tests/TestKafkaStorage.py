"""
    File: TestKafkaStorage.py
    Date: 08/2019
    Author: Spiros Politis
    Python: 3.6
"""

"""
    Test suite for KafkaStorage connector.
"""

import sys
import logging
import unittest

from TwitterWatchdog.Storage.KafkaStorage import KafkaStorage

# Set logging parameters
logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)


class TestKafkaStorage(unittest.TestCase):
    """
        Unit test for insert_one.
    """
    def test_insert_one(self):
        import time

        logging.debug("TestKafkaStorage:test_insert_one()")

        data = {
            "id": 1,
            "created_at": time.time(),
            "payload": "Test"
        }

        kafka_storage = KafkaStorage()

        kafka_storage.connect(
            endpoint={
                "bootstrap.servers": "localhost",
                "request.timeout.ms": 1000,
                "client.id": "twitter-watchdog",
                "api.version.request": True,
                "debug": "protocol,security"
            },
            topic="twitter-watchdog"
        )

        kafka_storage.insert_one(data)


if __name__ == "__main__":
    unittest.main()
