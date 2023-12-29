import sqlite3
import string
from flask import Flask, make_response,  g, request
import random
import base64
import zmq
import time
import math
import os
import messages_pb2
import time
import threading

"""
Utility Functions
"""
def get_db():
    if 'db' not in g:
        g.db = sqlite3.connect(
            'files.sqlite',
            detect_types=sqlite3.PARSE_DECLTYPES
        )
        g.db.row_factory = sqlite3.Row

    return g.db

def close_db(e=None):
    db = g.pop('db', None)

    if db is not None:
        db.close()

def random_string(length=8):
    """
    Returns a random alphanumeric string of the given length. 
    Only lowercase ascii letters and numbers are used.

    :param length: Length of the requested random string 
    :return: The random generated string
    """
    return ''.join([random.SystemRandom().choice(string.ascii_letters + string.digits) for n in range(length)])

app = Flask(__name__)
context = zmq.Context()
heartbeats = {}  # Dictionary to store heartbeat timestamps
app.teardown_appcontext(close_db)

def heartbeat_monitor():
    """Background thread function for monitoring heartbeats."""
    heartbeat_socket = context.socket(zmq.PULL)
    heartbeat_socket.bind("tcp://*:5556")

    while True:
        serialized_message = heartbeat_socket.recv()
        heartbeat = messages_pb2.heartbeat()
        heartbeat.ParseFromString(serialized_message)
        
        # Update the heartbeat dictionary
        print(f"Received heartbeat from node {heartbeat.node_id}")
        heartbeats[heartbeat.node_id] = heartbeat.timestamp

def check_heartbeats():
    """Regularly check for lost nodes."""
    heartbeat_interval = 5  # seconds
    heartbeat_tolerance = heartbeat_interval * 3

    while True:
        current_time = time.time()
        for node_id, last_heartbeat in list(heartbeats.items()):
            if current_time - last_heartbeat > heartbeat_tolerance:
                print(f"Node {node_id} is considered lost")
                del heartbeats[node_id]
        time.sleep(heartbeat_interval)


# Replication factor
k = 3

# Number of nodes
N = 20

# Function to create and connect sockets
def create_sockets(context, base_port, number_of_nodes):
    sockets = []
    for i in range(number_of_nodes):
        port = base_port + i
        socket = context.socket(zmq.PUSH)
        socket.connect(f"tcp://localhost:{port}")
        sockets.append(socket)
    return sockets

# Creating and connecting sockets for each data node
base_port = 5557
sockets = create_sockets(context, base_port, N)

# Socket to receive messages from Storage Nodes
response_socket = context.socket(zmq.PULL)
response_socket.bind("tcp://*:5553")
# Publisher socket for data request broadcasts
data_req_socket = context.socket(zmq.PUB)
data_req_socket.bind("tcp://*:5554")
# Wait for all workers to start and connect.
time.sleep(1)

# Endpoint for requesting files
@app.route('/files/<string:filename>',  methods=['GET'])
def download_file(filename):
    print(f"Downloading file {filename}")
    request = messages_pb2.getdata_request()
    request.filename = filename

    data_req_socket.send(request.SerializeToString())

    for _ in range(4):
        response = messages_pb2.getdata_response()
        response.ParseFromString(response_socket.recv())
        print(f"Received: {response.data} from file: {response.filename}")
    return make_response({'message': 'File downloaded successfully'}, 200)

# Endpoint for uploading files
# Splits file into 4 equal sized fragments and generates k full replicas on N different nodes
# Implemented a scheme for general k and N
@app.route('/files', methods=['POST'])
def add_files():
    # Check if file is present in the request
    file = request.files['file']
    file_data = file.read()
    if file_data is None:
        return make_response({'message': 'Missing file parameters'}, 400)
    
    # db = get_db()
    # cursor = db.execute("SELECT * FROM `Files` WHERE `Filename`=?", [file.filename])
    # if cursor.fetchone() is not None:
    #     return make_response({'message': 'File already exists'}, 400)

    # Extract the strategy parameter from the request
    strategy = request.form.get('strategy')
    if not strategy:
        return make_response({'message': 'Missing strategy parameter'}, 400)
    
    # Validate strategy
    valid_strategies = ['random', 'min_copysets', 'buddy']
    if strategy not in valid_strategies:
        return make_response({'message': 'Invalid strategy parameter'}, 400)

    # Test data
    # file_data = "abcdefghijklmnopqrstuvwxyz"

    # Split file into 4 equal sized fragments
    file_size = len(file_data)

    fragment_size = math.ceil(file_size / 4)
    fragments = []

    # Create fragments
    for i in range(0, file_size, fragment_size):
        fragments.append(file_data[i:i+fragment_size])

    # Generate k full replicas on N different nodes


    numberOfGroups = 6 # Number of groups to split the nodes into for buddy approach

    # Print fragments
    print(f"Fragments: {fragments}")
    print(f"k: {k} \nN: {N}")

    # Ensure there are enough nodes to form at least one copyset
    if N < k * len(fragments):
        print("Not enough nodes to form separate copysets for all fragments")
        return

    # Call the appropriate function based on the strategy
    storageFailed = False
    if strategy == 'random':
        storageFailed = random_placement(fragments, file_size, file.filename)
    elif strategy == 'min_copysets':
        storageFailed = min_copysets_placement(fragments, file_size, file.filename)
    elif strategy == 'buddy':
        storageFailed = buddy_approach(fragments, numberOfGroups, file_size, file.filename)

    if storageFailed:
        return make_response({'message': 'Storage failed'}, 500)
    return make_response({'message': 'File uploaded successfully'}, 201)
#
   
def random_placement(fragments, filesize, filename):
    # Shuffle the list of nodes
    nodes = list(range(N))
    random.shuffle(nodes)

    # Select k * fragments random nodes for replication
    replication_nodes = random.sample(nodes, k * len(fragments))

    db = get_db()
    cursor = db.execute(
        "INSERT INTO `Files`(`Filename`, `Size`, `ContentType`) VALUES (?,?,?)",
        (filename, filesize, 'binary')
    )
    db.commit()
    fileId = cursor.lastrowid

    # Send each fragment to k number of nodes
    for fragment in fragments:
        fragmentName = random_string()
        # Send the fragment to k number of nodes
        for i in range(k):
            # Select the next node from the list of replication nodes
            node = replication_nodes.pop()
            print(f"Sending fragment to node {node}: {fragment}")
            # Send the fragment to the node using the appropriate method
            
            send_data(node, fragment, fileId, fragmentName+".bin")  # Send to the first data node for testing




def min_copysets_placement(fragments, filesize, filename):
    # Create a list of node IDs
    nodes = list(range(N))

    # Create a list of nodes where each list contains k nodes
    replication_groups = [nodes[i:i + k] for i in range(0, len(nodes), k)]

    # While length of replication_groups is not divisible by k 
    while len(replication_groups) % k != 0:
        # Remove the last element
        replication_groups.pop()

    # Shuffle groups and select random replication group
    random.shuffle(replication_groups)
    random_group = random.choice(replication_groups)
    print("selected group: ", random_group)

    # Insert the File record in the DB
    db = get_db()
    cursor = db.execute(
        "INSERT INTO `Files`(`Filename`, `Size`, `ContentType`) VALUES (?,?,?)",
        (filename, filesize, 'binary')
    )
    db.commit()
    fileId = cursor.lastrowid

    # Send all fragments to each node in random_group
    for node in random_group:
        # Send each fragment to each node in the selected replication group
        for fragment in fragments:
            fragmentName = random_string()
            send_data(node, fragment, fileId, fragmentName+".bin")
            # Assuming the node index corresponds to the socket index
            print(f"Sending fragment to node {node}: {fragment}")
            

def buddy_approach(fragments, numberOfGroups, filesize, filename):
    # Create a list of node IDs
    nodes = list(range(N))
    random.shuffle(nodes)
    print(f"Nodes: {nodes}")

    numberOfNodesPerGroup = int(N / numberOfGroups)

    # Create a list of groups
    listOfGroups = [nodes[i:i+numberOfNodesPerGroup] for i in range(0, N, numberOfNodesPerGroup)]
    while len(listOfGroups) > numberOfGroups:
        listOfGroups.pop()  # Remove the last element
    print(f"List of Groups: {listOfGroups}")

    # Pick a random group of nodes from listOfGroups
    random_group = random.choice(listOfGroups)
    print(f"Random group of nodes selected: {random_group}")

    # Insert the File record in the DB
    db = get_db()
    cursor = db.execute(
        "INSERT INTO `Files`(`Filename`, `Size`, `ContentType`) VALUES (?,?,?)",
        (filename, filesize, 'binary')
    )
    db.commit()
    fileId = cursor.lastrowid

    # Send each fragment to k number of nodes in the selected group
    for fragment in fragments:
        selected_group = random_group.copy()
        fragmentName = random_string()
        # Send the fragment to k number of nodes in the selected group
        for _ in range(k):
            # Select the next node from the list of nodes in the selected group
            node = random.choice(selected_group)
            selected_group.remove(node)
            send_data(node, fragment, fileId, fragmentName+".bin")
            print(f"Sending fragment to node {node}: {fragment}")
            # Send the fragment to the node using the appropriate method


def send_data(node_id, filedata, fileId, filename="testfile.bin"):
    # Create a Protobuf message
    request = messages_pb2.storedata_request()
    request.filename = filename
    request.filedata = filedata  # Your dummy binary data

    # Serialize the Protobuf message
    serialized_request = request.SerializeToString()

    db = get_db()
    db.execute(
        "INSERT INTO `Fragments`(`FileID`, `FragmentName`, `NodeIDs`) VALUES (?,?,?)",
        (fileId, filename, node_id)
    )
    db.commit()

    # Send the serialized data
    sockets[node_id].send(serialized_request)
    print(f"Sent data to node {node_id}")

# Dummy data generation
def generate_dummy_data(size=1024):
    return os.urandom(size)  # Generates random binary data

if __name__ == '__main__':
    # Start heartbeat monitoring in a separate thread
    threading.Thread(target=heartbeat_monitor, daemon=True).start()
    threading.Thread(target=check_heartbeats, daemon=True).start()

    # Run Flask app
    app.run(host="localhost", port=5555)