#!/usr/bin/env python

from datetime import datetime
import json
import logging
import os
import sys
import socketio

url = os.getenv("WEB_SERVER")
sio = socketio.Client()

out_event = "get-suggestions"
in_event = "suggestions-list"

logging.basicConfig(level=logging.INFO)


@sio.event
def connect():
    print("Client connected")


@sio.event
def connect_error():
    print("The connection failed!")


@sio.event
def disconnect():
    print("I'm disconnected!")


sio.connect(url)
print("My id:", sio.sid)


@sio.on(in_event)
def handle_suggestions(message):
    """
    Handler for messages sent by the server to the "suggestion list" event.
    """
    elasped_time = (datetime.utcnow().timestamp() - message["start_ts_utc"]) * 1000
    logging.info(f"Took {elasped_time} ms")
    print(
        "Suggestions from server for msg id {}: {}".format(
            message["sequence_id"], message.get("suggestions")
        )
    )
    assert message["room"] == sio.sid, (message["room"], sio.sid)


def main():
    seq_id = 0
    while True:
        text = input("-->:")
        message = {
            "text": text,
            "stage": "send-request",
            "start_ts_utc": datetime.utcnow().timestamp(),
            "sequence_id": seq_id,
            "site": "stackoverflow",
        }
        sio.emit(out_event, json.dumps(message).encode("utf-8"))
        seq_id += 1


if __name__ == "__main__":
    main()
    sio.disconnect()
