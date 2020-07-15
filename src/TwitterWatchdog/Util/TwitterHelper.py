import logging
import asyncio

from .. import Globals as globals
from .. import TwitterBase

"""
    File: TwitterHelper.py
    Date: 08/2019
    Author: Spiros Politis
    Python: 3.6
"""

"""
    Helper class for the Twitter client.
"""


class TwitterHelper(TwitterBase.TwitterBase):
    """
        Constructor

        :param consumer_key: Twitter API consumer key.
        :param consumer_secret: Twitter API consumer secret.
        :param access_token: Twitter API access token.
        :param access_token_secret: Twitter API access token secret.
    """
    def __init__(self, consumer_key: str, consumer_secret: str, access_token: str, access_token_secret: str):
        super(TwitterHelper, self).__init__(consumer_key, consumer_secret, access_token, access_token_secret)

        # self.__queue = None
        # self.__interval = None

    
    """
        Retrieves trending topics for the provided WOE ID.
        
        :param WOEID: WOE ID
        
        :returns: array of strings representing topics.
    """
    def get_trends(self, woe_id: str):
        if globals.__DEBUG__:
            logging.debug("TwitterHelper:get_trends():woe_id:{}".format(woe_id))

        if woe_id is not None:
            trends = super().api.trends_place(id=woe_id)
        else:
            trends = super().api.trends_available()

        if globals.__DEBUG__:
            logging.debug("TwitterHelper:get_trends():trends:{}".format(trends))

        return trends
