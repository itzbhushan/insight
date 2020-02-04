#!/usr/bin/env python

from argparse import ArgumentParser, FileType
from datetime import datetime
import json
import logging
import os
import sys
import socketio
from time import sleep

url = os.getenv("WEB_SERVER")
sio = socketio.Client()

out_event = "get-suggestions"
in_event = "suggestions-list"

out_file = None

logging.basicConfig(level=logging.INFO)


@sio.event
def connect():
    logging.info("Client connected")


@sio.event
def connect_error():
    logging.info("The connection failed!")


@sio.event
def disconnect():
    out_file.close()
    logging.info("I'm disconnected!")


@sio.on(in_event)
def handle_suggestions(message):
    """
    Handler for messages sent by the server to the "suggestion list" event.
    """
    message["timestamps"].append(datetime.utcnow().timestamp())
    elapsed_time = (message["timestamps"][-1] - message["timestamps"][0]) * 1000
    seq_id = message["sequence_id"]
    timestamps_str = ", ".join([f"{ts:.2f}" for ts in message["timestamps"]])
    logging.info(f"Received msg {seq_id}. Took {elapsed_time:.2f} ms.")
    print(f"{seq_id}, {timestamps_str}", file=out_file, flush=True)


def main():
    parser = ArgumentParser("you-complete-me client")
    parser.add_argument(
        "file", help="Input file with list of questions", type=FileType("r")
    )
    parser.add_argument(
        "--out-dir", help="Output dir where metrics are stored", default="/tmp"
    )
    parser.add_argument(
        "--rate",
        help="Messages per second. Defaults to 1 msg/sec. Set to zero to disable rate limiting.",
        default=1.0,
        type=float,
    )
    parser.add_argument(
        "--num-messages",
        help="Number of messags to send. Defaults to reading the entire file.",
        default=0,
        type=int,
    )

    args = parser.parse_args()

    body_length = 100
    seq_id = 0

    if args.rate == 0:
        rate_limit = False
    else:
        rate_limit = True
        sleep_time = 1.0 / args.rate

    sio.connect(url)
    logging.debug(f"My client id: {str(sio.sid)}")

    out_file_path = os.path.join(args.out_dir, sio.sid)
    global out_file
    out_file = open(out_file_path, "a")
    print(
        "msg_id, client_send, server_req, es_start, es_end, pg_start, pg_end, server_rsp, client_rsp",
        file=out_file,
    )
    logging.info(f"Saving metrics to {out_file_path}.")

    for line in args.file:
        l_json = json.loads(line)
        if "body" not in l_json or "site" not in l_json:
            continue
        message = {
            "text": l_json["body"][0:body_length],  # for now limit to 100 characters.
            "stage": "send-request",
            "timestamps": [datetime.utcnow().timestamp()],
            "sequence_id": seq_id,
            "site": l_json["site"],
        }
        sio.emit(out_event, json.dumps(message).encode("utf-8"))
        logging.debug(f"Sending message: {seq_id}")
        seq_id += 1

        if args.num_messages and seq_id >= args.num_messages:
            break

        if rate_limit:
            sleep(sleep_time)


if __name__ == "__main__":
    main()
    sio.disconnect()
