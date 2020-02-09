#!/usr/bin/env python

from argparse import ArgumentParser
from datetime import datetime
import json
import logging
import os

from elasticsearch import Elasticsearch
from elasticsearch.exceptions import ConnectionTimeout

from pulsar import Client
from pulsar import ConsumerType

logging.basicConfig(level=logging.WARN)


def msg_received_callback(status, msg_id):
    """
    Function invoked when the message is acknowledged by the broker.

    params:
    ------
    status: Publishing status (usually "ok").
    msg_id: Message identifier.
    """
    logging.debug(f"Message {msg_id} result = {status}")


def select_query_type(type, field, text, site):
    query = {}
    query["match"] = {
        "query": {
            "bool": {
                "must": [{"match": {field: text}}],
                "filter": [{"term": {"site": site}}],
            }
        }
    }

    query["mlt"] = {
        "query": {
            "bool": {
                "must": [
                    {
                        "more_like_this": {
                            # search the body of existing questions (not title).
                            "fields": [field],
                            "like": text,
                            "min_term_freq": 1,
                            "max_query_terms": 20,
                        }
                    }
                ],
                "filter": [{"term": {"site": site}}],
            }
        }
    }

    assert type in query
    return query[type]


def find_suggestions(es, in_topic, out_topic, client, args):
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
        if "timestamps" in packet:
            packet["timestamps"].append(datetime.utcnow().timestamp())
        logging.debug(
            f"NLP-er: Received message {packet['text']}, id={msg_id}, room={packet['room']}"
        )

        # Responses from ES only need to include a subset of keys.
        # For example, we don't need to return the "body" of the message.
        # Body is large, takes time to (de)serialize.
        keys_to_return = ["id", "title"]

        es_query = {
            "_source": keys_to_return,
            "size": args.limit_result_count,  # number of results to limit.
            **select_query_type(
                args.query_type, args.field, packet["text"], packet["site"]
            ),
        }

        results = {}
        try:
            # In order to optimize ES performance, we want to experiment with
            # different indexing schemes. For example, instead of one common
            # index for the entire dataset, we could have an index per stackexchange
            # subdomain (or site) as they call it. In order to experiment with
            # different indexing options, pass the name of the index to the
            # consumer in the body of the message. If no index is present, use
            # the default index.
            index = packet.get("index", args.index)
            logging.info(f"Using {index} index.")
            response = es.search(index=index, body=es_query)
        except ConnectionTimeout as e:
            logging.exception(
                f"Read timed out. Skipping read for index={index}, query={es_query}"
            )
        else:  # N.B try/else clause.
            packet["total_hits"] = response["hits"]["total"]["value"]
            for hit in response["hits"]["hits"]:
                title, score, id = (
                    hit["_source"]["title"],
                    hit["_score"],
                    hit["_source"]["id"],
                )
                results[id] = {"title": title, "score": score}

        packet["suggestions"] = results
        if "timestamps" in packet:
            packet["timestamps"].append(datetime.utcnow().timestamp())
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
    parser.add_argument(
        "--field", help="ES document to search. Defaults to body.", default="body"
    )
    parser.add_argument(
        "--query-type", help="ES query type", default="match", choices=["match", "mlt"]
    )
    # 10K is the default max_result_window limit in ES, but we only need 10 usually.
    parser.add_argument(
        "--limit-result-count", help="Limit ES result count.", default=10, type=int
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
    out_topic = "curate-topic"
    es = Elasticsearch(args.es_url)
    find_suggestions(es, in_topic, out_topic, client, args)


if __name__ == "__main__":
    main()
