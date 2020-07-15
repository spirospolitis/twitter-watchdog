"""
    File: TestFileStorage.py
    Date: 08/2019
    Author: Spiros Politis
    Python: 3.6
"""

"""
    Test suite for FileStorage connector.
"""

import sys
import logging
import unittest

from TwitterWatchdog.Storage.FileStorage import FileStorage

# Set logging parameters
logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)


class TestFileStorage(unittest.TestCase):
    """
        Unit test for insert_one.
    """
    def test_insert_one(self):
        import time

        logging.debug('TestFileStorage:test_insert_one()')

        data = {
            "id": 1,
            "created_at": time.time(),
            "payload": "Test"
        }

        file_storage = FileStorage()

        file_storage.connect(
            filename='../data/test.json',
            mode='a+',
            encoding='utf-8'
        )

        file_storage.insert_one(data)


if __name__ == '__main__':
    unittest.main()
