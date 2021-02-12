import json
import os
import requests
import time

from kafka import KafkaProducer


def fetch() -> dict:
    endpoint = 'https://api.coindesk.com/v1/bpi/currentprice.json'
    res = requests.get(endpoint)
    res_json = json.loads(res.text)
    return res_json


def clean_record(raw: dict) -> dict:
    return {
        'time': raw['time']['updated'],
        'currenty': raw['bpi']['USD']['code'],
        'rate': raw['bpi']['USD']['rate_float']
    }


def publish_record(producer: KafkaProducer) -> None:
    res = fetch()
    cleaned_response = clean_record(res)

    producer.send('bpi-raw-stream', cleaned_response)
    

if __name__ == '__main__':
    KAFKA_BROKER_URL = os.environ.get('KAFKA_BROKER_URL')

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER_URL,
        value_serializer=lambda x: json.dumps(x).encode('utf-8')
        )

    while True:
        publish_record(producer)
        producer.flush()
        time.sleep(1)
