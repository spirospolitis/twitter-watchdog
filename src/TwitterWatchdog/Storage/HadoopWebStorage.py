from . import AbstractStorage

from pywebhdfs.webhdfs import PyWebHdfsClient

"""
    File: HadoopWebStorage.py
    Date: 08/2019
    Author: Spiros Politis
    Python: 3.6
"""

"""
    Implements a Web HDFS storage interface.
"""


class HadoopWebStorage(AbstractStorage.AbstractStorage, PyWebHdfsClient):
    """
        Constructor
    """
    def __init__(self):
        AbstractStorage.AbstractStorage.__init__(self)

    """
        Connects to Hadoop cluster.

        :param url: Hadoop cluster URL.
        :param user: Hadoop user.
        :param filename: Filename.
        :param encoding: File encoding.
    """
    def connect(self, host:str = 'localhost', port:str = '50070', user:str = None, file_name:str = None):
        PyWebHdfsClient.__init__(self, host = host, port = port, user_name = user, timeout = 1)
        
        self.create_file(file_name, None)

        # self.__client = InsecureClient(url, user = user)
        # self.__file = self.__client.write(filename, encoding = encoding)

        # self.__file.write('{"test":"test"}')

    """
        Disconnects from the Hadoop cluster.
    """
    def disconnect(self):
        # self.__client.disconnect()

        pass

    """
        Inserts one document in the specified file.

        :param item: Item to write to file.
    """
    def insert_one(self, item:{}):
        import json

        pass

        #self.__client.write('model.json', json.dumps(item))

        # json.dump(item, self.__file)
        
        # self.__file.flush()

    """
        Properties
    """
    # @property
    # def client(self):
    #     return self