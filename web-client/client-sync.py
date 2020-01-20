from time import sleep
import socketio

url = "http://ec2-18-237-144-210.us-west-2.compute.amazonaws.com:8000"
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
    print("Event name: message. Message from server:", data)


@sio.on("my response")
def handle_response(data):
    print("Event name: my response. Message from server:", data)


for i in range(100):
    sio.emit("message", "Hello server" + str(i))
    sio.emit("my event", "Hello my event in server" + str(i))
    sleep(0.2)

sio.disconnect()
