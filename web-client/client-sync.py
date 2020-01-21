#!/usr/bin/env python

from time import sleep
import socketio

url = "http://ec2-54-202-236-123.us-west-2.compute.amazonaws.com:8000"
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
def handle_suggestions(data):
    """
    Handler for messages sent by the server to the "suggestion list" event.
    """
    print("Event name: suggestion list. Message from server:", data)


while True:
    message = input("-->")
    sio.emit(out_event, message)
    # sio.emit("my event", "Hello my event in server" + str(i))

sio.disconnect()
