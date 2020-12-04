import yaml
config = dict(
    search_tweets_api = dict(
        account_type = 'premium',
        endpoint = 'https://api.twitter.com/1.1/tweets/search/30day/test.json',
        consumer_key = 'q32sDHyagqAPDhrqClHb3Q4wq',
        consumer_secret = '8vH0j0mN8t71DlI9QiYb1WtTsh7M6tmQ7f1ZrJ8Wq62whFSA1w'
    )
)
with open('twitter_keys_fullarchive.yaml', 'w') as config_file:
    yaml.dump(config, config_file, default_flow_style=False)
