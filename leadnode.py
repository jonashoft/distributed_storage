import sqlite3
import string
from flask import Flask, make_response,  g, request, jsonify
import random
import base64
import zmq
import time
import math
import os
import messages_pb2
import time
import threading
import sys


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

def clear_database():
    db = get_db()
    try:
        db.execute("DELETE FROM Fragments")
        db.execute("DELETE FROM Files")
        db.commit()
    finally:
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
lost_nodes = []  # List of lost nodes
lock = threading.Lock() # Lock for shared resources
app.teardown_appcontext(close_db)

k = 3
N = 12

def heartbeat_monitor():
    """Background thread function for monitoring heartbeats."""
    heartbeat_socket = context.socket(zmq.PULL)
    heartbeat_socket.bind("tcp://*:5556")

    while True:
        serialized_message = heartbeat_socket.recv()
        heartbeat = messages_pb2.heartbeat()
        heartbeat.ParseFromString(serialized_message)
        print(f"Received heartbeat from node {heartbeat.node_id}")

        with lock:  # Acquire lock before modifying shared resources
            heartbeats[heartbeat.node_id] = heartbeat.timestamp
            # Remove node from lost_nodes list if it's back online
            if heartbeat.node_id in lost_nodes:
                lost_nodes.remove(heartbeat.node_id)

def check_heartbeats():
    """Regularly check for lost nodes."""
    heartbeat_interval = 1  # seconds
    heartbeat_tolerance = heartbeat_interval * 3

    while True:
        current_time = time.time()
        with lock:  # Acquire lock before accessing shared resources
            for node_id, last_heartbeat in list(heartbeats.items()):
                if current_time - last_heartbeat > heartbeat_tolerance:
                    print(f"Node {node_id} is considered lost")
                    if node_id not in lost_nodes:
                        lost_nodes.append(node_id)
                    # Remove the node from the heartbeats dictionary
                    del heartbeats[node_id]
        time.sleep(heartbeat_interval)

def calculate_lost_files():
    db = get_db()
    total_files = db.execute("SELECT COUNT(*) FROM `Files`").fetchone()[0]
    if total_files == 0:
        return "No files in the system."

    # Fetch all fragment-node mappings for each file
    fragment_mappings = db.execute("SELECT `FileID`, `FragmentNumber`, `NodeIDs` FROM `Fragments`").fetchall()

    # Dynamically organize fragments by file based on actual FileIDs
    file_fragment_availability = {}
    for mapping in fragment_mappings:
        file_id, fragment_number, node_id = mapping
        if file_id not in file_fragment_availability:
            file_fragment_availability[file_id] = {str(i): False for i in range(1, 5)}
        if node_id not in lost_nodes:
            file_fragment_availability[file_id][fragment_number] = True

    # Check for lost files
    lost_files_count = sum(not all(fragments.values()) for fragments in file_fragment_availability.values())

    # Calculate the fraction of lost files
    fraction_lost = lost_files_count / total_files if total_files > 0 else 0
    return f"Fraction of Lost Files: {fraction_lost:.2%}"


@app.route('/lost_files_fraction', methods=['GET'])
def get_lost_files_fraction():
    try:
        lost_files_fraction = calculate_lost_files()
        return jsonify({"fraction_of_lost_files": lost_files_fraction}), 200
    except Exception as e:
        # Handling any exceptions that might occur
        return jsonify({"error": str(e)}), 500



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

    db = get_db()
    cursor = db.execute("SELECT * FROM `Files` WHERE `Filename`=?", [filename])
    if not cursor: 
        return make_response({"message": "Error connecting to the database"}, 500)
    
    file = cursor.fetchone()
    if not file:
        return make_response({"message": f"File {filename} not found"}, 404)
    
    f = dict(file)
    db = get_db()
    cursor = db.execute("SELECT * FROM `Fragments` WHERE `FileID`=?", [f['FileID']])
    fragments = cursor.fetchall()
    fragmentFiles = []

    for fragment in fragments:
        f = dict(fragment)
        request = messages_pb2.getdata_request()
        request.filename = f['FragmentName']

        data_req_socket.send(request.SerializeToString())
        
        response = messages_pb2.getdata_response()
        response.ParseFromString(response_socket.recv())
        print(f"Received: file: {response.filename}")
        fragmentFiles.append(response.filedata)
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

    # Extract the strategy parameter from the request
    strategy = request.form.get('strategy')
    if not strategy:
        return make_response({'message': 'Missing strategy parameter'}, 400)
    
    # Validate strategy
    valid_strategies = ['random', 'min_copysets', 'buddy']
    if strategy not in valid_strategies:
        return make_response({'message': 'Invalid strategy parameter'}, 400)

    # Split file into 4 equal sized fragments
    file_size = len(file_data)

    fragment_size = math.ceil(file_size / 4)
    fragments = []

    # Create fragments
    for i in range(0, file_size, fragment_size):
        fragments.append(file_data[i:i+fragment_size])


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
        storageFailed = buddy_approach(fragments, file_size, file.filename)

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
    fragmentNumber = 0

    # Send each fragment to k number of nodes
    for fragment in fragments:
        fragmentNumber += 1
        fragmentName = random_string()
        # Send the fragment to k number of nodes
        for i in range(k):
            # Select the next node from the list of replication nodes
            node = replication_nodes.pop()
            print(f"Sending fragment to node {node}: {fragment}")
            # Send the fragment to the node using the appropriate method
            
            send_data(node, fragment, fileId, fragmentNumber, fragmentName+".bin")  # Send to the first data node for testing




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
    fragmentNumber = 0

    # Send all fragments to each node in random_group
    for node in random_group:
        # Send each fragment to each node in the selected replication group
        for fragment in fragments:
            fragmentNumber += 1
            fragmentName = random_string()
            send_data(node, fragment, fileId, fragmentNumber, fragmentName+".bin")
            # Assuming the node index corresponds to the socket index
            print(f"Sending fragment to node {node}: {fragment}")
        fragmentNumber = 0
            

def buddy_approach(fragments, filesize, filename):

    # Generate k full replicas on N different nodes
    # Calculate numberOfGroups based on N and k
    numberOfGroups = max(1, N // k) # For buddy 


    # Create a list of node IDs
    nodes = list(range(N))
    random.shuffle(nodes)
    print(f"Nodes: {nodes}")

    numberOfNodesPerGroup = int(N / numberOfGroups)
    if numberOfNodesPerGroup < k:
        print("Not enough nodes in each group to satisfy the replication factor")
        return True  # Indicates a failure in storage

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
    fragmentNumber = 0

    # Send each fragment to k number of nodes in the selected group
    for fragment in fragments:
        fragmentNumber += 1
        selected_group = random_group.copy()
        fragmentName = random_string()
        # Send the fragment to k number of nodes in the selected group
        for _ in range(k):
            # Select the next node from the list of nodes in the selected group
            node = random.choice(selected_group)
            selected_group.remove(node)
            send_data(node, fragment, fileId, fragmentNumber, fragmentName+".bin")
            print(f"Sending fragment to node {node}: {fragment}")
            # Send the fragment to the node using the appropriate method


def send_data(node_id, filedata, fileId, fragmentNumber, filename="testfile.bin"):
    # Create a Protobuf message
    request = messages_pb2.storedata_request()
    request.filename = filename
    request.filedata = filedata  # Your dummy binary data

    # Serialize the Protobuf message
    serialized_request = request.SerializeToString()

    db = get_db()
    db.execute(
        "INSERT INTO `Fragments`(`FileID`, `FragmentName`, `FragmentNumber`, `NodeIDs`) VALUES (?,?,?,?)",
        (fileId, filename, fragmentNumber, node_id)
    )
    db.commit()

    # Send the serialized data
    sockets[node_id].send(serialized_request)
    print(f"Sent data to node {node_id}")

# Dummy data generation
def generate_dummy_data(size=1024):
    return os.urandom(size)  # Generates random binary data

if __name__ == '__main__':
    # Default values for k and N
    k = 3  # Replication factor
    N = 12  # Number of nodes
    
    # Check if command-line arguments are provided
    if len(sys.argv) >= 3:
        k = int(sys.argv[1])
        N = int(sys.argv[2])

    # Create an application context
    with app.app_context():
        # Wipe the database clean
        clear_database()

    # Start heartbeat monitoring in a separate thread
    threading.Thread(target=heartbeat_monitor, daemon=True).start()
    threading.Thread(target=check_heartbeats, daemon=True).start()

    # Run Flask app
    app.run(host="localhost", port=5555)