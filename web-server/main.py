from flask import Flask, render_template
from flask_socketio import SocketIO, emit, send

app = Flask(__name__)
app.config["SECRET_KEY"] = "secret!"
socketio = SocketIO(app)


@app.route("/")
def index():
    return "Hello world!"


@socketio.on("message")
def handle_message(message):
    print("Event name: message. Message from client:", message)
    send(message + "Hello client, this is server")


@socketio.on("my event")
def test_message(message):
    print("Event name: my event. Message from client:", message)
    emit("my response", {"data": "got it!", "message": message})


if __name__ == "__main__":
    socketio.run(app, host="0.0.0.0", port=8000, debug=True)
