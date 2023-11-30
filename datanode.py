import sys
import zmq

def redirect_output(log_file_path):
    sys.stdout = open(log_file_path, 'a', buffering=1)
    sys.stderr = sys.stdout

def start_data_node(node_id, port, log_file_path):
    redirect_output(log_file_path)

    context = zmq.Context()
    socket = context.socket(zmq.PULL)
    socket.bind(f"tcp://*:{port}")

    print(f"Data node {node_id} started on port {port}")

    while True:
        message = socket.recv()
        print(f"Data node {node_id} received: {message}")

if __name__ == "__main__":
    node_id = sys.argv[1]
    port = sys.argv[2]
    log_file_path = sys.argv[3]
    start_data_node(node_id, port, log_file_path)
