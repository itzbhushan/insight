#!/usr/bin/env python

from argparse import ArgumentParser
from datetime import datetime
import json
import logging
import os

from pulsar import Client
from pulsar import ConsumerType

from sqlalchemy import create_engine, and_
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm.query import Query

from tables import Questions

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


def init_postgres_session(connection):
    """
    Setup access to postgres.

    A function that sets up acces to postgres and returns a session
    object. Other functions can use the session object and run
    queries as needed.
    """
    engine = create_engine("postgresql://{user}:{pwd}@{url}/{db}".format(**connection))
    Session = sessionmaker(bind=engine)
    return Session()


def curate(suggestions, site, session):
    """
    Score search suggestions based on question metadata.

    ES scores documents based on the term statistics (tf-idf). However,
    better suggestions can be provided if we take into account some
    metadata about the question itself. This function curates (i.e. scores)
    results by scaling the ES score with the number of answers for that
    question.
    """
    results = (
        session.query(Questions)
        .filter(and_(Questions.site == site, Questions.id.in_(suggestions.keys())))
        .all()
    )
    for q in results:
        # Retain original score for question with answer_count == 0. Hence +1.
        suggestions[str(q.id)]["score"] *= q.answer_count + 1
        suggestions[str(q.id)]["link"] = q.link

    ranked_suggestions = sorted(
        suggestions.values(), key=lambda x: x["score"], reverse=True
    )
    return ranked_suggestions


def find_suggestions(in_topic, out_topic, client, session):
    """
    Consume from in_topic, process and produce to out_topic.

    This function reads messages from curate-topic. Messages
    contain question id, ES score and the name of the subdomain.
    The curator then queries postgres, orders results and
    sends back the curated message to the user.
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
            f"Curator: Received message {packet['text']}, id={msg_id}, room={packet['room']}"
        )
        if len(packet["suggestions"]):
            curated_result = curate(packet["suggestions"], packet["site"], session)
            packet["suggestions"] = curated_result

        if "timestamps" in packet:
            packet["timestamps"].append(datetime.utcnow().timestamp())
        producer.send_async(json.dumps(packet).encode("utf-8"), msg_received_callback)


def main():
    parser = ArgumentParser(
        "Pulsar consumers curating results by connecting to postgres."
    )
    parser.add_argument(
        "--pulsar-broker-url",
        help="URL of pulsar broker.",
        default=os.getenv("PULSAR_BROKER_URL"),
    )
    args = parser.parse_args()

    if not args.pulsar_broker_url:
        parser.error(
            "Pulsar broker url is null. Set PULSAR_BROKER_URL environment variable."
        )

    connection_map = {
        "user": "POSTGRES_USER",
        "pwd": "POSTGRES_PWD",
        "url": "POSTGRES_URL",
        "db": "POSTGRES_DB",
    }
    connection = {k: os.getenv(v) for k, v in connection_map.items()}
    session = init_postgres_session(connection)
    client = Client(args.pulsar_broker_url)
    in_topic = "curate-topic"
    out_topic = "suggestions-topic"
    find_suggestions(in_topic, out_topic, client, session)


if __name__ == "__main__":
    main()
