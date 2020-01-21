#!/usr/bin/env python

import json
from threading import Thread

from flask import Flask, render_template, request
from flask_socketio import SocketIO, emit, send

from pulsar import Client
from pulsar import ConsumerType

app = Flask(__name__)
app.config["SECRET_KEY"] = "secret!"
socketio = SocketIO(app)

in_event = "get-suggestions"
out_event = "suggestions-list"

pulsar_broker_url = "pulsar://ec2-54-212-169-175.us-west-2.compute.amazonaws.com:6650"
out_topic = "suggest-topic"
in_topic = "suggestions-topic"

client = Client(pulsar_broker_url)
producer = client.create_producer(out_topic)


@app.route("/")
def index():
    """
    Default route.
    """
    return "Hello world!"


@socketio.on(in_event)
def get_suggestions(data):
    """
    _get suggestions_ event handler.

    This function is called when clients send message to the
    get suggestions event.
    """
    packet = {"text": data + "Hello client", "room": request.sid}
    print(
        f"Event name: get suggestions. Message from client is {data} from {request.sid}"
    )
    producer.send(json.dumps(packet).encode("utf-8"))
    # send("Hello client")


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
        print(
            f"Web-server: Received message {packet['text']}, id={msg_id}, room={packet['room']}"
        )
        with app.test_request_context("/"):
            socketio.emit(out_event, {"data": packet["room"]}, room=packet["room"])


if __name__ == "__main__":
    consumer_thread = Thread(
        target=consumer_thread, args=(client, in_topic), daemon=True
    )
    consumer_thread.start()
    socketio.run(app, host="0.0.0.0", port=8000, debug=True)
    print("Closing server connection")
    client.close()
    print("Closing pulsar client")
