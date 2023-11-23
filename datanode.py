import zmq
import base64

# Create a ZeroMQ context
context = zmq.Context()

# Create a PULL socket
socket = context.socket(zmq.PULL)

# Connect to the lead node
socket.connect("tcp://localhost:5556")  # Replace with the actual address of your lead node

# Keep receiving and printing messages
while True:
    print("Hej!")
    message = socket.recv()
    

    print("Received message!")