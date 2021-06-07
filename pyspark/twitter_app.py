import pathlib
from datetime import datetime as dt
from uuid import uuid4

import requests
import os
import json
from secrets import twitter_secrets as ts

OUTPUT_DIR = '<...>'

i = 3
counter = 0
STOP_AFTER = 5000

# To set your enviornment variables in your terminal run the following line:
# export 'BEARER_TOKEN'='<your_bearer_token>'


def create_headers(bearer_token):
    headers = {"Authorization": "Bearer {}".format(bearer_token)}
    return headers


def get_rules(headers, bearer_token):
    response = requests.get(
        "https://api.twitter.com/2/tweets/search/stream/rules", headers=headers
    )
    if response.status_code != 200:
        raise Exception(
            "Cannot get rules (HTTP {}): {}".format(response.status_code, response.text)
        )
    print(json.dumps(response.json()))
    return response.json()


def delete_all_rules(headers, bearer_token, rules):
    if rules is None or "data" not in rules:
        return None

    ids = list(map(lambda rule: rule["id"], rules["data"]))
    payload = {"delete": {"ids": ids}}
    response = requests.post(
        "https://api.twitter.com/2/tweets/search/stream/rules",
        headers=headers,
        json=payload
    )
    if response.status_code != 200:
        raise Exception(
            "Cannot delete rules (HTTP {}): {}".format(
                response.status_code, response.text
            )
        )
    print(json.dumps(response.json()))


def set_rules(headers, delete, bearer_token):
    # You can adjust the rules if needed
    sample_rules = [
        {"value": "ethereum", "tag": "ethereum"},
        {"value": "binance coin", "tag": "binancecoin"},
        {"value": "dogecoin", "tag": "dogecoin"},
        {"value": "cardano", "tag": "cardano"},
        {"value": "chainlink", "tag": "chainlink"},
        {"value": "XRP", "tag": "XRP"},
        {"value": "litecoin", "tag": "litecoin"},
        {"value": "stellar", "tag": "stellar"},
        {"value": "tether", "tag": "tether"},
        {"value": "bitcoin", "tag": "bitcoin"},
        {"value": "polkadot", "tag": "polkadot"},
    ]
    payload = {"add": sample_rules}
    response = requests.post(
        "https://api.twitter.com/2/tweets/search/stream/rules",
        headers=headers,
        json=payload,
    )
    if response.status_code != 201:
        raise Exception(
            "Cannot add rules (HTTP {}): {}".format(response.status_code, response.text)
        )
    print(json.dumps(response.json()))


def get_stream(headers, set, bearer_token):
    response = requests.get(
        "https://api.twitter.com/2/tweets/search/stream", headers=headers, stream=True,
    )
    print(response.status_code)
    if response.status_code != 200:
        raise Exception(
            "Cannot get stream (HTTP {}): {}".format(
                response.status_code, response.text
            )
        )
    for response_line in response.iter_lines():
        global counter
        counter = counter + 1
        if counter == STOP_AFTER:
            break
        if response_line:
            json_response = json.loads(response_line)
            print(json.dumps(json_response, indent=4, sort_keys=True))
 
            with pathlib.Path(OUTPUT_DIR) / f"{dt.now().timestamp()}_{uuid4()}.json" as F:
                F.write_bytes(response_line)

            
def main():
    # bearer_token = os.environ.get("BEARER_TOKEN")
    bearer_token = ts.BEARER_TOKEN
    headers = create_headers(bearer_token)
    rules = get_rules(headers, bearer_token)
    delete = delete_all_rules(headers, bearer_token, rules)
    set = set_rules(headers, delete, bearer_token)
    get_stream(headers, set, bearer_token)


if __name__ == "__main__":
    main()