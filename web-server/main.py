#!/usr/bin/env python
import json
import threading

from flask import Flask, render_template
from flask_socketio import SocketIO, emit, send

from pulsar import Client
from pulsar import ConsumerType

app = Flask(__name__)
app.config["SECRET_KEY"] = "secret!"
socketio = SocketIO(app)

pulsar_broker_url = "pulsar://ec2-54-212-169-175.us-west-2.compute.amazonaws.com:6650"
topic = "suggest-topic"

client = Client(pulsar_broker_url)
producer = client.create_producer(topic)
consumer_out = open("consume.txt", "a")


@app.route("/")
def index():
    """
    Default route.
    """
    return "Hello world!"


@socketio.on("message")
def handle_message(data):
    """
    message - event handler.

    This function is called when client sends messages to the
    message event.
    """

    print("Event name: message. Message from client:", data)
    # send message to pulsar broker
    producer.send(data.encode("utf-8"))
    # Return message back to client. We can later figure out
    # how to send message to client via the pulsar consumer.
    send(data + "Hello client, this is server")


def consumer_thread(client):
    """
    Pulsar consumer thread.

    Eventually the consumer will be a separate application, but for
    now we simulate consumers using this thread. In the next iteration,
    we will send messages back to the client via this consumer. For
    now, all consumed messages are written to a text file (consume.txt)
    """

    consumer = client.subscribe(
        topic, "test-subscription", consumer_type=ConsumerType.Shared
    )

    while True:
        msg = consumer.receive()
        data = msg.data().decode("utf-8")
        msg_id = msg.message_id()
        print(f"Received message {data}, id={msg_id}", file=consumer_out)
        # emit("my response", {"data": data})
        consumer.acknowledge(msg)


if __name__ == "__main__":
    consumer_thread = threading.Thread(
        target=consumer_thread, args=(client,), daemon=True
    )
    consumer_thread.start()
    socketio.run(app, host="0.0.0.0", port=8000, debug=True)
    print("Closing server connection")
    client.close()
    print("Closing pulsar client")
    consumer_out.close()
    print("closing file")
