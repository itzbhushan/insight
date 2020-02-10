#!/usr/bin/env python

from argparse import ArgumentParser
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

out_file = None

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


@sio.on(in_event)
def handle_suggestions(message):
    """
    Handler for messages sent by the server to the "suggestion list" event.
    """
    message["timestamps"].append(datetime.utcnow().timestamp())
    elasped_time = (message["timestamps"][-1] - message["timestamps"][0]) * 1000
    num_suggestions = len(message.get("suggestions", []))
    logging.info(f"Took {elasped_time} ms. Returned {num_suggestions} suggestions.")
    msg_id = message["sequence_id"]
    text = message["text"]
    suggestions = message.get("suggestions", [])
    for suggestion in suggestions:
        print(
            f"{msg_id}, {text}, {suggestion['title']}, {suggestion['score']}",
            file=out_file,
            flush=True,
        )


def main():
    parser = ArgumentParser("you-complete-me client")
    parser.add_argument(
        "--out-dir", help="Output dir where suggestions are stored", default="/tmp"
    )
    args = parser.parse_args()

    sio.connect(url)
    logging.debug(f"My id:", sio.sid)

    out_file_path = os.path.join(args.out_dir, sio.sid)
    logging.info(f"Saving suggestions to {out_file_path}")

    global out_file
    out_file = open(out_file_path, "a")
    print("msg_id, question_text, suggestion_title, score", file=out_file)

    seq_id = 0
    try:
        while True:
            text = input("-->:")
            message = {
                "text": text,
                "stage": "send-request",
                "timestamps": [datetime.utcnow().timestamp()],
                "sequence_id": seq_id,
                "site": "stackoverflow",
            }
            sio.emit(out_event, message)
            seq_id += 1
    except KeyboardInterrupt:
        out_file.close()


if __name__ == "__main__":
    main()
    sio.disconnect()
