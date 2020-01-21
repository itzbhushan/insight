#!/usr/bin/env python

import json

from pulsar import Client
from pulsar import ConsumerType


def find_suggestions(in_topic, out_topic, client):
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
        print(
            f"NLP-er: Received message {packet['text']}, id={msg_id}, room={packet['room']}"
        )
        ## NLP logic goes here.
        producer.send(data.encode("utf-8"))


if __name__ == "__main__":
    pulsar_broker_url = (
        "pulsar://ec2-54-212-169-175.us-west-2.compute.amazonaws.com:6650"
    )
    client = Client(pulsar_broker_url)
    in_topic = "suggest-topic"
    out_topic = "suggestions-topic"
    find_suggestions(in_topic, out_topic, client)
