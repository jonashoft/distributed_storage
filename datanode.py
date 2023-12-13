import signal
import sys
import zmq
import os
from messages_pb2 import storedata_request
import messages_pb2

context = zmq.Context()
socket = context.socket(zmq.PULL)
node_id = None

# Setup signal handler
def signal_handler(sig, frame):
    print(f'Shutting down node {node_id}...')
    # Close sockets and terminate ZMQ context
    socket.close()
    context.term()
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)


def redirect_output(log_file_path):
    sys.stdout = open(log_file_path, 'a', buffering=1)
    sys.stderr = sys.stdout


def start_data_node(node_id, port, log_file_path):
    redirect_output(log_file_path)
    
    socket.bind(f"tcp://*:{port}")

    sender = context.socket(zmq.PUSH)
    sender.connect("tcp://localhost:5553")

    subscriber = context.socket(zmq.SUB)
    subscriber.connect("tcp://localhost:5554")
    subscriber.setsockopt(zmq.SUBSCRIBE, b'')

    print(f"Data node {node_id} started on port {port}")

    poller = zmq.Poller()
    poller.register(socket, zmq.POLLIN)
    poller.register(subscriber, zmq.POLLIN)
    while True:
        try:
        # Poll all sockets
            socks = dict(poller.poll())
        except KeyboardInterrupt:
            break
        # At this point one or multiple sockets have received a message
        if socket in socks:
            message = socket.recv()
            
            # Parse protobuf message
            request = storedata_request()
            request.ParseFromString(message)
            file_name = request.filename

            # Ensure directory exists
            os.makedirs(os.path.join('data', str(node_id)), exist_ok=True)

            # Save file content as bin and send back name of bin file
            file_path = os.path.join('data', str(node_id), file_name)
            
            with open(file_path, "wb") as file:
                file.write(message)
            
            print(f"Data node {node_id} received: {message}")
        if subscriber in socks:
            request = messages_pb2.storedata_request()
            request.ParseFromString(subscriber.recv())
            print(f"Data chunk request: {request.filename}")
            response = messages_pb2.getdata_response()
            response.filename = request.filename
            with open(request.filename, "rb") as file:
                print(f"Found chunk {file}, sending it back")
                response.data = file.read()
            # response.data = b"test123"
            sender.send(response.SerializeToString())


def save_data_to_folder(data):
    pass


if __name__ == "__main__":
    node_id = sys.argv[1]
    port = sys.argv[2]
    log_file_path = sys.argv[3]
    start_data_node(node_id, port, log_file_path)


