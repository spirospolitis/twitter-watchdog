# Twitter Watchdog

A application to monitor Twitter for tweets, based on topics.

## Configuration

Application configuration is performed by the config.json file. The parameters that can be specified are the following:

- **twitter_authentication** section: specifies the application Twitter authentication parameters. Note that you should have a Twitter application setup in order to retrieve the tokens required. Parameters are as follows:

    - **consumer_key**: Twitter consumer key.
    - **consumer_secret**: Twitter consumer secret.
    - **access_token**: Twitter access token.
    - **access_token_secret**: Twitter access token secret.

- **tweepy**: parameters that govern the operation of the Tweepy library. Parameters are as follows:

    - **async**: true / false, governs the operation of Tweepy in synchronous or asynchronous mode.
    - **wait_on_rate_limit**: true / false, specifies whether the Tweepy library has a backoff threshold applied, when Twitter imposes rate limiting.

- **storage**: this section contains all the parameters that govern the persistence of received data to different connectors. Connectors included are the following:

    - **File storage**: plain local filesystem connector with the following parameters:
        - **type**: (string, required) file
        - **options.file_path**: (string, required) The folder in which to store Tweet files. Note that the directory specified is created if it does not exist. Files have an auto-generated file name which is the timestamp at file creation.
        - **options.format**: (string, required) File format. Currently supported JSON, CSV.
        - **enabled**: (boolean, required) Controls whether this adapter is enabled.

    - **Hadoop storage**: a connector for storing Tweet files on Hadoop.
        - **type**: (string, required) hadoop.
        - **enabled**: (boolean, required) Controls whether this adapter is enabled.

    - **Kafka storage**: a connector for writing Tweets to a Kafka topic. Parameters include the following:
        - **type**: (string, required) kafka
        - **options.endpoint.bootstrap.servers**: (string, required) The Kafka endpoint server.
        - **options.endpoint.request.timeout.ms**: (int, optional) connection request timeout (millis).
        - **options.endpoint.client.id**: (string, optional) The Kafka client ID, usually "twitter-watchdog".
        - **options.endpoint.api.version.request**: (boolean, required).
        - **options.endpoint.debug**: (boolean, optional) Debug options for the Kafka connector.
        - **options.topic**: (string, required) The Kafka topic to which to write data to.
        - **enabled**: (boolean, required) controls whether this adapter is enabled.

    - **Mongo DB storage**:
        - **type**: (string, required) mongodb.
        - **options.host**: (string, required) Mongo DB host.
        - **options.port**: (int, required) Mongo DB port.
        - **options.username**: (string, required) Mongo DB application username. Note that the user must have been previously created in Mongo.
        - **options.password**: (string, required) Mongo DB application password.
        - **options.auth_source**: (string, optional) Mongo DB authentication source, usually "admin".
        - **options.auth_mechanism**: (string, optional) Default is "SCRAM-SHA-256".
        - **options.db**: (string, required) The database in which to store Tweets.
        - **options.collection**: (string, required) The collection in which to store Tweets.
        - **enabled**: (boolean, required) Controls whether this adapter is enabled.

- **languages**: (null or array of strings, required) Tweet languages to watch for.
- **locality**: (null or string, required) If locality is provided (e.g. "Athens"), the program retrieves the WOE ID (Where-On-Earth ID) of the location from the Yahoo! API which is subsequently used to retrieved tweets for the region specified.
- **topics**: (null or array of strings, required) If an array of topics is provided, the program retrieves tweets pertaining to these topics. Otherwise, trending topic tweets are received.
- **debug**: (boolean(boolean, required)) Controls whether to print debug information to stdout.
- **verbose**: (boolean, required) Controls the verbosity level.

## Unit testing

```bash
python -m unittest discover
```

## Running (standalone)

```bash
python main.py -p <unix_process_name> -c <path_to_config>
```

## Running (Docker container)

```bash
docker run --name twitter-watchdog -d twitter-watchdog:latest
```
