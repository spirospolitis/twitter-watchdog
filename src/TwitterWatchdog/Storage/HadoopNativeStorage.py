from . import AbstractStorage

"""
    File: HadoopNativeStorage.py
    Date: 08/2019
    Author: Spiros Politis
    Python: 3.6
"""

"""
    Implements a native HDFS storage interface. Uses the Apache Arror HDFS implementation (pyarrow.hdfs).

    Requires *nix like OS.
"""


class HadoopNativeStorage(AbstractStorage.AbstractStorage):
    """
        Constructor
    """
    def __init__(self):
        import pyarrow

        AbstractStorage.AbstractStorage.__init__(self)

        self.__client:pyarrow.HadoopFileSystem = None
        self.__file:pyarrow.HdfsFile = None

    """
        Connects to Hadoop cluster.

        :param url: Hadoop cluster URL.
        :param user: Hadoop user.
        :param filename: Filename.
        :param encoding: File encoding.
    """
    def connect(self, host: str = 'localhost', port: str = '50070', user: str = None, kerb_ticket=None, driver: str = 'libhdfs3', path: str = None):
        import pyarrow

        self.__client = pyarrow.hdfs.connect(host=host, port=port, user=user, kerb_ticket=kerb_ticket, driver=driver)
        self.__file = self.__client.open(path, 'a+')

    """
        Disconnects from the Hadoop cluster.
    """
    def disconnect(self):
        self.__file.flush()
        self.__file.close()

    """
        Inserts one document in the specified file.

        :param item: Item to write to file.
    """
    def insert_one(self, item:{}):
        self.__file.write(item)
        self.__file.flush()

    """
        Properties
    """
    @property
    def client(self):
        return self.__client

    @property
    def file(self):
        return self.__file
