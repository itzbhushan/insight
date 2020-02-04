#!/usr/bin/env python

from argparse import ArgumentParser
import json
import os
import logging
from threading import Lock

from flask import Flask, render_template, request
from flask_socketio import SocketIO, emit, send

from pulsar import Client
from pulsar import ConsumerType

app = Flask(__name__)
app.config["SECRET_KEY"] = "secret!"
socketio = SocketIO(app, async_mode="threading")
# socketio = SocketIO(app, async_mode="eventlet")

in_event = "get-suggestions"
out_event = "suggestions-list"

pulsar_broker_url = os.getenv("PULSAR_BROKER_URL")
out_topic = "suggest-topic"
in_topic = "suggestions-topic"

client = None
producer = None

thread = None
thread_lock = Lock()
loopback = False

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


@socketio.on(in_event)
def get_suggestions(data):
    """
    _get suggestions_ event handler.

    This function is called when clients send message to the
    get suggestions event.
    """
    data_dict = json.loads(data.decode("utf-8"))
    data_dict["room"] = request.sid
    data_dict["suggestions"] = loopback_suggestions(data_dict["text"])
    logging.debug(f"Message from client {request.sid} is {data_dict}")
    if loopback:
        socketio.emit(out_event, data_dict, room=request.sid)
    else:
        producer.send_async(
            json.dumps(data_dict).encode("utf-8"), msg_received_callback
        )


def loopback_suggestions(text):
    suggestions = {str(i): {"title": text + str(i), "score": i} for i in range(10)}
    return suggestions


def consumer_thread(client, in_topic):
    """
    Pulsar consumer thread.

    This web-server thread consumes messages from the given topic and emits
    a list of suggestions back to the client.
    """
    consumer = client.subscribe(
        in_topic, "test-subscription", consumer_type=ConsumerType.Shared
    )

    while True:
        msg = consumer.receive()
        consumer.acknowledge(msg)
        msg_id = msg.message_id()
        data = msg.data().decode("utf-8")
        packet = json.loads(data)
        logging.debug(
            f"Web-server: Received message {packet['text']}, id={msg_id}, room={packet['room']}"
        )
        socketio.emit(out_event, packet, room=packet["room"])


@socketio.on("connect")
def test_connect():
    if loopback:
        return

    global thread
    with thread_lock:
        if thread is None:
            thread = socketio.start_background_task(consumer_thread, client, in_topic)


def main():
    parser = ArgumentParser("Web-server")
    parser.add_argument("--loopback", action="store_true", help="Loop back mode")
    args = parser.parse_args()

    global loopback
    loopback = args.loopback

    if not loopback:
        global client, producer
        client = Client(pulsar_broker_url)
        producer = client.create_producer(out_topic)

    socketio.run(app, host="0.0.0.0", debug=True)
    print("Closing server connection")


if __name__ == "__main__":
    main()
