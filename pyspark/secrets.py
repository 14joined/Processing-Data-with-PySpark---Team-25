CONSUMER_KEY = "<...>"
CONSUMER_SECRET = "<...>"

ACCESS_TOKEN = "<...>"
ACCESS_SECRET = "<...>"

BEARER_TOKEN = "<...>"


class TwitterSecrets:

    def __init__(self):
        self.CONSUMER_KEY = CONSUMER_KEY
        self.CONSUMER_SECRET = CONSUMER_SECRET
        self.ACCESS_TOKEN = ACCESS_TOKEN
        self.ACCESS_SECRET = ACCESS_SECRET
        self.BEARER_TOKEN = BEARER_TOKEN

        # Tests if keys are present
        for key, secret in self.__dict__.items():
            assert secret != "", f"Please provide a valid secret for: {key}"


twitter_secrets = TwitterSecrets()