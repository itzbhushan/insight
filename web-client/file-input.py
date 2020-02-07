#!/usr/bin/env python

from argparse import ArgumentParser, FileType
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
num_messages = 0


@sio.event
def connect():
    logging.info("Client connected")


@sio.event
def connect_error():
    logging.info("The connection failed!")


@sio.event
def disconnect():
    logging.info("I'm disconnected!")


@sio.on(in_event)
def handle_suggestions(message):
    """
    Handler for messages sent by the server to the "suggestion list" event.
    """
    message["timestamps"].append(datetime.utcnow().timestamp())
    timestamps_str = ", ".join([f"{ts:.2f}" for ts in message["timestamps"]])
    elapsed_time = (message["timestamps"][-1] - message["timestamps"][0]) * 1000
    seq_id = message["sequence_id"]
    site = message["site"]
    hits = message["total_hits"]
    logging.info(f"Received msg {seq_id}. Took {elapsed_time:.2f} ms.")
    print(f"{site}, {hits}, {seq_id}, {timestamps_str}", file=out_file)

    if seq_id == num_messages:
        # Exit the program by disconnecting from server, which unblocks the main function.
        sio.disconnect()


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
        help="Number of messags to send. Defaults to 1.",
        default=1,
        type=int,
    )

    args = parser.parse_args()

    body_length = 100
    seq_id = 1

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
        "site, hits, msg_id, client_send, server_req, es_start, es_end, pg_start, pg_end, server_rsp, client_rsp",
        file=out_file,
    )
    global num_messages
    num_messages = args.num_messages

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
        sio.emit(out_event, message)
        logging.debug(f"Sending message: {seq_id}")
        seq_id += 1

        if num_messages and seq_id > num_messages:
            break

        if rate_limit:
            sio.sleep(sleep_time)
    else:
        # for/else. Code reaches here when for loop ends naturally.
        # i.e. number of lines in file < num_messages. In which case
        # set num_messages to the last seq_id.
        num_messages = seq_id

    logging.debug(f"{num_messages} messages sent to server.")
    sio.wait()  # wait until disconnect from server.
    out_file.close()
    logging.info(f"Processed all messages. Saved metrics to {out_file_path}. Exiting.")


if __name__ == "__main__":
    main()
    sio.disconnect()
