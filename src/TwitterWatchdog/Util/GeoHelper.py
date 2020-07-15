import asyncio
import logging

import yweather

from .. import Globals as globals

"""
    File: GeoHelper.py
    Date: 08/2019
    Author: Spiros Politis
    Python: 3.6
"""

"""
    Some geo helper functions.
"""


class GeoHelper:
    """
        Constructor
    """
    def __init__(self):
        pass


    """
        Gets a Where-on-Earth ID (WOE ID) from the Yahoo! API.
        
        :param place: the string representation of a place.
        
        :returns: WOE ID.
    """
    def get_woe_id(self, place: str):
        if globals.__DEBUG__:
            logging.debug("GeoHelper:get_woe_id():place:{}".format(place))

        woeid = None

        try:
            client = yweather.Client()
            
            woeid = client.fetch_woeid(place)
        except Exception as error:
            logging.error("GeoHelper:get_woe_id():error:{}".format(error))

            raise Exception(error)

        return woeid
