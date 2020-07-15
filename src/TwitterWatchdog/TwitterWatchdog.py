import logging

from . import Globals as globals
from . import TwitterBase

"""
    File: TwitterWatchdog.py
    Date: 08/2019
    Author: Spiros Politis
    Python: 3.6
"""

"""
    Twitter streaming.
"""


class TwitterWatchdog(TwitterBase.TwitterBase):
    import tweepy

    """
        Constructor
    """
    def __init__(self, consumer_key: str, consumer_secret: str, access_token: str, access_token_secret: str, storage: [], wait_on_rate_limit: bool = False):
        import tweepy

        super(TwitterWatchdog, self).__init__(consumer_key, consumer_secret, access_token, access_token_secret, wait_on_rate_limit)

        self.__storage = storage

        self.__twitterStreamListener = self.TwitterStreamListener(self.__storage)
        self.__twitterStream = tweepy.Stream(auth=super().api.auth, listener=self.__twitterStreamListener, tweet_mode='extended')

    """
        https://developer.twitter.com/en/docs/tweets/filter-realtime/guides/basic-stream-parameters.html
        
        Standard streaming API request parameters:
            - delimited
            - stall_warnings
            - filter_level
            - language
            - follow
            - track
            - locations
            - count
            - with (deprecated)
            - replies (deprecated)
            - stringify_friend_id (deprecated)
    """
    def connect(self, track: [] = None, languages: [] = None, is_async: bool = False):
        if globals.__DEBUG__:
            logging.debug("TwitterWatchdog:connect():track:{}:languages:{}:is_async:{}".format(track, languages, is_async))

        self.__twitterStream.filter(track=track, languages=languages, is_async=is_async)

    """
        Stops the streaming process.
    """
    def disconnect(self):
        if globals.__DEBUG__:
            logging.debug("TwitterWatchdog:disconnect()")

        self.__twitterStream.disconnect()

    """
        Extends the Tweepy StreamListener class.
    """
    class TwitterStreamListener(tweepy.StreamListener):
        def __init__(self, storage: []):
            super(TwitterWatchdog.TwitterStreamListener, self).__init__()
            
            self.__storage = storage

        """
            Callback for "on data received" event.
            
            :param data: payload.
        """
        def on_status(self, data: object):
            import logging
            import json

            original_tweet = False
            retweet = False
            quoted_retweet = False

            # Retweet.
            # A retweet without any further user input (quote).
            if hasattr(data, "retweeted_status"):
                retweet = True
                
                try:
                    tweet_text = data.retweeted_status.extended_tweet["full_text"]
                except AttributeError:
                    tweet_text = data.retweeted_status.text
            
            # Quoted retweet.
            # Twitter has one more special case: the quote tweet. 
            # This occurs when a user retweets a post and adds a comment alongside the retweet. 
            # As before, this case is signified by a "quoted_status" attribute.
            elif hasattr(data, "quoted_status"):
                quoted_retweet = True

                try:
                    tweet_text = data.quoted_status.extended_tweet["full_text"]
                except AttributeError:
                    tweet_text = data.quoted_status.text
            
            # Original tweet.
            else:
                original_tweet = True

                try:
                    tweet_text = data.extended_tweet["full_text"]
                except AttributeError:
                    tweet_text = data.text

            if "hashtags" in data.entities:
                hashtags = data.entities["hashtags"]
            else:
                hashtags = None

            if "urls" in data.entities:
                urls = data.entities["urls"]
            else:
                urls = None

            if "mentions" in data.entities:
                mentions = data.entities["mentions"]
            else:
                mentions = None

            tweet = {
                "tweet": {
                    "id": data.id, 
                    "created_at": str(data.created_at), 
                    "text": tweet_text.encode("utf-8").decode("utf-8"), 
                    "source": data.source, 
                    "in_reply_to_status_id": data.in_reply_to_status_id, 
                    "in_reply_to_user_id": data.in_reply_to_user_id, 
                    "in_reply_to_screen_name": data.in_reply_to_screen_name, 
                    "quote_count": data.quote_count, 
                    "reply_count": data.reply_count, 
                    "retweet_count": data.retweet_count, 
                    "favorite_count": data.favorite_count, 
                    "entities_hashtags": hashtags, 
                    "entities_urls": urls, 
                    "entities_mentions": mentions, 
                    "original": original_tweet, 
                    "retweet": retweet, 
                    "quoted_retweet": quoted_retweet, 
                    "lang": data.lang
                }, 
                "user": {
                    "id": data.user.id, 
                    "name": data.user.name, 
                    "screen_name": data.user.screen_name, 
                    "location": data.user.location, 
                    "followers_count": data.user.followers_count, 
                    "friends_count": data.user.friends_count, 
                    "listed_count": data.user.listed_count, 
                    "favourites_count": data.user.favourites_count, 
                    "statuses_count": data.user.statuses_count, 
                    "created_at": str(data.user.created_at),
                    "following": data.user.following
                }
            }
            
            if self.__storage is not None:
                for storage_interface in self.__storage:
                    storage_interface.insert_one(tweet)
            else:
                if globals.__VERBOSE__:
                    logging.info("TwitterWatchdog::StreamListener::{}".format(json.dumps(tweet, ensure_ascii=False).encode("utf8")))
        
        """
            Callback of "on error" event.
            
            :param error_code: Twitter API error code.
        """
        def on_error(self, error_code: int):
            import logging
            
            # Returning False in on_error disconnects the stream.
            # Returning non-False reconnects the stream, with backoff.
            if error_code == 420:
                logging.warning("TwitterWatchdog::StreamListener::Twitter throttling imposed, error_code={}".format(error_code))
                
                return True
            else:
                logging.error("TwitterWatchdog::StreamListener::Twitter error, error_code={}".format(error_code))
                
                return True
