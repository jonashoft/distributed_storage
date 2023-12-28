import signal
import sys
import zmq
import os
from messages_pb2 import storedata_request, getdata_request, getdata_response, heartbeat
import messages_pb2
import time
import threading

context = zmq.Context()
receiver = context.socket(zmq.PULL)
sender = context.socket(zmq.PUSH)
heartbeat_socket = context.socket(zmq.PUSH)

node_id = None
data_folder = os.path.join('data', str(node_id))

# Setup signal handler
def signal_handler(sig, frame):
    print(f'Shutting down node {node_id}...')
    # Close sockets and terminate ZMQ context
    receiver.close()
    context.term()
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)


def redirect_output(log_file_path):
    sys.stdout = open(log_file_path, 'a', buffering=1)
    sys.stderr = sys.stdout


def start_data_node(node_id, port, log_file_path):
    redirect_output(log_file_path)
    
    receiver.bind(f"tcp://*:{port}")
    sender.connect("tcp://localhost:5553")

    subscriber = context.socket(zmq.SUB)
    subscriber.connect("tcp://localhost:5554")
    subscriber.setsockopt(zmq.SUBSCRIBE, b'')

    print(f"Data node {node_id} started on port {port}")

    # Start heartbeat thread
    heartbeat_thread = threading.Thread(target=send_heartbeat, args=(node_id,))
    heartbeat_thread.daemon = True
    heartbeat_thread.start()

    poller = zmq.Poller()
    poller.register(receiver, zmq.POLLIN)
    poller.register(subscriber, zmq.POLLIN)

    while True:
        try:
        # Poll all sockets
            socks = dict(poller.poll())
        except KeyboardInterrupt:
            break

        # At this point one or multiple sockets have received a message
        if receiver in socks:
            storedata_message = receiver.recv_multipart()
            handle_storedata_request(storedata_message)

        if subscriber in socks:
            getdata_message = subscriber.recv_multipart()
            handle_getdata_request(getdata_message)

def handle_storedata_request(message):
    # Parse protobuf message
    request = storedata_request()
    request.ParseFromString(message)
    file_name = request.filename
    file_data = request.filedata

    # Ensure directory exists
    os.makedirs(os.path.join('data', str(node_id)), exist_ok=True)

    # Save file content and send back name of bin file
    file_path = os.path.join(data_folder, file_name)
    with open(file_path, "wb") as file:
        file.write(file_data)
    print(f"Saved {file_name} to {file_path}")

def handle_getdata_request(message):
    # Parse protobuf message
    request = getdata_request()
    request.ParseFromString(message)
    file_name = request.filename
    print(f"Data chunk request: {file_name}")

    response = messages_pb2.getdata_response()
    response.filename = file_name

    # Try to load the requested file from the local file system,
    # send response only if found
    try:
        file_path = os.path.join(data_folder, file_name)
        with open(file_path, "rb") as in_file:
            print(f"Found chunk {file_name}, sending it back")
            response.filedata = in_file.read()
            sender.send(response.SerializeToString())
    except FileNotFoundError:
        # The chunk is not stored by this node
        print(f"Did not find chunk {file_name}")
        pass

def send_heartbeat(node_id, interval=5):
    heartbeat_socket.connect("tcp://localhost:5556")  # Specify the correct address and port

    while True:
        heartbeat_message = heartbeat()
        heartbeat_message.node_id = node_id
        heartbeat_message.timestamp = time.time()
        print(f"Sending heartbeat from node {node_id} at time {heartbeat_message.timestamp}")
        heartbeat_socket.send(heartbeat_message.SerializeToString())
        time.sleep(interval)

if __name__ == "__main__":
    node_id = sys.argv[1]
    port = sys.argv[2]
    log_file_path = sys.argv[3]

    start_data_node(node_id, port, log_file_path)


