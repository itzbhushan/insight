#!/usr/bin/env python

from argparse import ArgumentParser
import json
import logging
import os

from elasticsearch import Elasticsearch

from pulsar import Client
from pulsar import ConsumerType

logging.basicConfig(level=logging.INFO)


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

        # Responses from ES only need to include a subset of keys.
        # For example, we don't need to return the "body" of the message.
        # Body is large, takes time to (de)serialize.
        keys_to_return = ["id", "title", "site"]

        query = {
            "_source": keys_to_return,
            "query": {
                "bool": {
                    "must": [
                        {
                            "more_like_this": {
                                "fields": ["body"],
                                "like": packet["text"],
                                "min_term_freq": 1,
                                "max_query_terms": 20,
                            }
                        }
                    ],
                    "filter": [{"term": {"site": packet["site"]}}],
                }
            },
        }
        response = es.search(index=es_index, body=query)
        results = []
        for hit in response["hits"]["hits"]:
            title, score = hit["_source"]["title"], hit["_score"]
            results.append({"title": title, "score": score})
        packet["suggestions"] = results
        producer.send_async(json.dumps(packet).encode("utf-8"), msg_received_callback)


def main():
    parser = ArgumentParser("Pulsar consumers searching ES.")
    parser.add_argument(
        "--index", help="ES index to search", default=os.getenv("ES_INDEX")
    )
    parser.add_argument(
        "--pulsar-broker-url",
        help="URL of pulsar broker.",
        default=os.getenv("PULSAR_BROKER_URL"),
    )
    parser.add_argument(
        "--es-url", help="Elastic Search URL", default=os.getenv("ES_URL")
    )
    args = parser.parse_args()

    if not args.index:
        parser.error(
            "Empty ES index. Update ES_INDEX environment variable with index name."
        )

    if not args.pulsar_broker_url:
        parser.error(
            "Pulsar broker url is null. Set PULSAR_BROKER_URL environment variable."
        )

    if not args.es_url:
        parser.error("ES url is null. Set ES_URL environment variable.")

    client = Client(args.pulsar_broker_url)
    in_topic = "suggest-topic"
    out_topic = "suggestions-topic"
    es = Elasticsearch(args.es_url)
    find_suggestions(es, in_topic, out_topic, client, args.index)


if __name__ == "__main__":
    main()
