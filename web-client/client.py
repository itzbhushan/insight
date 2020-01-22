#!/usr/bin/env python
import sys
from time import sleep
import socketio

url = "http://ec2-54-202-236-123.us-west-2.compute.amazonaws.com:5000"
sio = socketio.Client()

out_event = "get-suggestions"
in_event = "suggestions-list"


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
    print("Event name: suggestion list. Message from server:", message)
    assert message["room"] == sio.sid, (message["room"], sio.sid)


def main():
    counter = 0
    client_id = sys.argv[1]
    wait_time = int(sys.argv[2])
    msg_prefix = f"Client: {client_id}. Packet:"
    while True:
        sleep(wait_time)
        counter += wait_time
        sio.emit(out_event, msg_prefix + str(counter // wait_time))


if __name__ == "__main__":
    main()
    sio.disconnect()
