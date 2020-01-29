#!/usr/bin/env python

from argparse import ArgumentParser
import json
import logging
import os

from elasticsearch import Elasticsearch

from pulsar import Client
from pulsar import ConsumerType

logging.basicConfig(level=logging.INFO)
ES_INDEX = "so-questions2"


def msg_received_callback(status, msg_id):
    """
    Function invoked when the message is acknowledged by the broker.

    params:
    ------
    status: Publishing status (usually "ok").
    msg_id: Message identifier.
    """
    logging.debug(f"Message {msg_id} result = {status}")


def find_suggestions(es, in_topic, out_topic, client, es_index):
    """
    Consume from in_topic, process and produce to out_topic.

    This function reads messages from in_topic, performs NLP and queries the
    elastic search server to find posts related to the incoming messages. These
    suggestions are then posted to out_topic for other consumers.
    """
    consumer = client.subscribe(
        in_topic, "test-subscription", consumer_type=ConsumerType.Shared
    )
    producer = client.create_producer(out_topic)
    while True:
        msg = consumer.receive()
        consumer.acknowledge(msg)
        msg_id = msg.message_id()
        data = msg.data().decode("utf-8")
        packet = json.loads(data)
        logging.debug(
            f"NLP-er: Received message {packet['text']}, id={msg_id}, room={packet['room']}"
        )
        ## NLP logic goes here.
        query = {
            "query": {
                "more_like_this": {
                    "fields": ["title"],
                    "like": packet["text"],
                    "min_term_freq": 1,
                    "max_query_terms": 20,
                }
            }
        }
        response = es.search(index=es_index, body=query)
        title = ""
        for hit in response["hits"]["hits"]:
            title = "\n".join([title, hit["_source"]["title"]])
        packet["suggestions"] = title
        producer.send_async(json.dumps(packet).encode("utf-8"), msg_received_callback)


def main():
    parser = ArgumentParser("Pulsar consumers searching ES.")
    parser.add_argument("--index", help="ES index to search", default=ES_INDEX)
    args = parser.parse_args()

    pulsar_broker_url = os.getenv("PULSAR_BROKER_URL")
    client = Client(pulsar_broker_url)
    in_topic = "suggest-topic"
    out_topic = "suggestions-topic"
    es = Elasticsearch(os.getenv("ES_URL"))
    find_suggestions(es, in_topic, out_topic, client, args.index)


if __name__ == "__main__":
    main()
