from time import sleep
import socketio

url = "http://ec2-35-161-161-253.us-west-2.compute.amazonaws.com:8000"
sio = socketio.Client()


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


@sio.event
def message(data):
    """
    Handler for messages sent by the server to the "message" event.
    """
    print("Event name: message. Message from server:", data)


@sio.on("my response")
def handle_response(data):
    """
    Handler for messages sent by the server to the "my response" event.
    """
    print("Event name: my response. Message from server:", data)


for i in range(1):
    sio.emit("message", "Hello server" + str(i))
    # sio.emit("my event", "Hello my event in server" + str(i))
    sleep(0.2)

sio.disconnect()
