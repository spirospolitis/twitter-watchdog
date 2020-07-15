from abc import ABC, abstractmethod

"""
    File: AbstractStorage.py
    Date: 08/2019
    Author: Spiros Politis
    Python: 3.6
"""

"""
    Abstract base class for classes that implement a storage interface.
"""


class AbstractStorage(ABC):
    """
        Constructor
    """
    def __init__(self):
        super().__init__()
    
    """
        Defines the connection interface.
    """
    @abstractmethod
    def connect(self):
        pass

    """
        Defines the disconnection interface.
    """
    @abstractmethod
    def disconnect(self):
        pass

    """
        Defines the interface for committing one item to storage.
    """
    @abstractmethod
    def insert_one(self, item: {}):
        pass
