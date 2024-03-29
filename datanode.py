import signal
import sys
import zmq
import os
from messages_pb2 import storedata_request, getdata_request, getdata_response, heartbeat
import time
import threading

context = zmq.Context()
receiver = context.socket(zmq.PULL)
sender = context.socket(zmq.PUSH)
heartbeat_socket = context.socket(zmq.PUSH)

node_id = None
data_folder = os.path.join('data')

def signal_handler(sig, frame):
    print(f'Shutting down node {node_id}...')
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

    heartbeat_thread = threading.Thread(target=send_heartbeat, args=(node_id,))
    heartbeat_thread.daemon = True
    heartbeat_thread.start()

    poller = zmq.Poller()
    poller.register(receiver, zmq.POLLIN)
    poller.register(subscriber, zmq.POLLIN)

    while True:
        try:
            socks = dict(poller.poll())
        except KeyboardInterrupt:
            break
        
        if receiver in socks:
            print("Received storedata request")
            storedata_message = receiver.recv()
            handle_storedata_request(storedata_message)

        if subscriber in socks:
            print("Received getdata request")
            getdata_message = subscriber.recv()
            handle_getdata_request(getdata_message)

def handle_storedata_request(message):
    request = storedata_request()
    request.ParseFromString(message)
    file_name = request.filename
    file_data = request.filedata

    os.makedirs(os.path.join('data', str(node_id)), exist_ok=True)
    
    file_path = os.path.join(data_folder, str(node_id), file_name)
    with open(file_path, "wb") as file:
        file.write(file_data)
    print(f"Saved {file_name} to {file_path}")

def handle_getdata_request(message):
    request = getdata_request()
    request.ParseFromString(message)
    file_name1 = request.fragmentName1
    file_name2 = request.fragmentName2
    file_name3 = request.fragmentName3
    file_name4 = request.fragmentName4
    print(f"Data chunk request: {file_name1}, {file_name2}, {file_name3}, {file_name4}")

    response = getdata_response()

    foundAFragment = False

    try:
        file_path = os.path.join(data_folder, str(node_id), file_name1)
        with open(file_path, "rb") as in_file:
            print(f"Found chunk {file_name1}, sending it back")
            response.fragmentName1 = file_name1
            response.fragmentData1 = in_file.read()
            foundAFragment = True
    except FileNotFoundError:
        print(f"Did not find chunk {file_name1}")
        pass

    try:
        file_path = os.path.join(data_folder, str(node_id), file_name2)
        with open(file_path, "rb") as in_file:
            print(f"Found chunk {file_name2}, sending it back")
            response.fragmentName2 = file_name2
            response.fragmentData2 = in_file.read()
            foundAFragment = True
    except FileNotFoundError:
        print(f"Did not find chunk {file_name2}")
        pass

    try:
        file_path = os.path.join(data_folder, str(node_id), file_name3)
        with open(file_path, "rb") as in_file:
            print(f"Found chunk {file_name3}, sending it back")
            response.fragmentName3 = file_name3
            response.fragmentData3 = in_file.read()
            foundAFragment = True
    except FileNotFoundError:
        print(f"Did not find chunk {file_name3}")
        pass

    try:
        file_path = os.path.join(data_folder, str(node_id), file_name4)
        with open(file_path, "rb") as in_file:
            print(f"Found chunk {file_name4}, sending it back")
            response.fragmentName4 = file_name4
            response.fragmentData4 = in_file.read()
            foundAFragment = True
    except FileNotFoundError:
        print(f"Did not find chunk {file_name4}")
        pass
    if foundAFragment:
        print("Found a fragment")
        sender.send(response.SerializeToString())

def send_heartbeat(node_id, interval=240):
    heartbeat_socket.connect("tcp://localhost:5556")

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


