"""
    File: TwitterBase.py
    Date: 08/2019
    Author: Spiros Politis
    Python: 3.6
"""

"""
    Base class of Twitter client.
"""


class TwitterBase(object):
    """
        Constructor
    """
    def __init__(self, consumer_key: str, consumer_secret: str, access_token: str, access_token_secret: str, wait_on_rate_limit: bool = False):
        import tweepy
        
        self.__consumer_key = consumer_key
        self.__consumer_secret = consumer_secret
        self.__access_token = access_token
        self.__access_token_secret = access_token_secret
        
        self.__auth = tweepy.OAuthHandler(self.__consumer_key, self.__consumer_secret)
        self.__auth.set_access_token(self.__access_token, self.__access_token_secret)
        
        self.__api = tweepy.API(self.__auth, wait_on_rate_limit=wait_on_rate_limit)
        
    """
        Properties
    """
    @property
    def consumer_key(self):
        return self.__consumer_key
    
    @property
    def consumer_secret(self):
        return self.__consumer_secret
    
    @property
    def access_token(self):
        return self.__access_token
    
    @property
    def access_token_secret(self):
        return self.__access_token_secret
    
    @property
    def auth(self):
        return self.__auth
    
    @property
    def api(self):
        return self.__api
