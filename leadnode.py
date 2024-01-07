import sqlite3
import string
from flask import Flask, make_response,  g, request, jsonify
import random
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
        g.db = sqlite3.connect( 'files.sqlite', detect_types=sqlite3.PARSE_DECLTYPES )
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
    return ''.join([random.SystemRandom().choice(string.ascii_letters + string.digits) for n in range(length)])

app = Flask(__name__)
context = zmq.Context()
heartbeats = {}  # Dictionary to store heartbeat timestamps
lost_nodes = []  # List of lost nodes
lock = threading.Lock() # Lock for shared resources
app.teardown_appcontext(close_db)

if len(sys.argv) >= 3:
    print("Using command-line arguments")
    k = int(sys.argv[1])
    N = int(sys.argv[2])
else:
    k = 3
    N = 24  

numberOfGroups = 2 # For buddy 

nodes = list(range(N))
random.shuffle(nodes)

numberOfNodesPerGroup = int(N / numberOfGroups)

listOfGroups = [nodes[i:i+numberOfNodesPerGroup] for i in range(0, N, numberOfNodesPerGroup)]
while len(listOfGroups) > numberOfGroups:
    listOfGroups.pop()  # Remove the last element until the length of the list is equal to numberOfGroups

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

@app.route('/get_metrics', methods=['GET'])
def get_metrics():
    return make_response({"k": k, 'N':N}, 200)

@app.route('/lost_files_fraction', methods=['GET'])
def get_lost_files_fraction():
    try:
        lost_files_fraction = calculate_lost_files()
        return jsonify({"fraction_of_lost_files": lost_files_fraction}), 200
    except Exception as e:
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

poller = zmq.Poller()
poller.register(response_socket, zmq.POLLIN)

# Wait for all workers to start and connect.
time.sleep(1)

@app.route('/filename_by_id/<int:id>',  methods=['GET'])
def get_file_name_from_id(id):
    db = get_db()
    cursor = db.execute("SELECT * FROM `Files` WHERE `FileID`=?", [id])
    if not cursor: 
        return make_response({"message": "Error connecting to the database"}, 500)
    
    file = cursor.fetchone()
    if not file:
        return make_response({"message": f"File with ID: {id} not found", 'fileId':id}, 404)
    return make_response({"filename": dict(file)["Filename"]}, 200)

@app.route('/file_by_id/<int:id>',  methods=['GET'])
def download_file_by_id(id):
    start_time = time.time()
    print(f"Downloading file from id {id}")
    db = get_db()
    cursor = db.execute("SELECT * FROM `Files` WHERE `FileID`=?", [id])
    if not cursor: 
        return make_response({"message": "Error connecting to the database"}, 500)
    
    file = cursor.fetchone()
    if not file:
        return make_response({"message": f"File with ID: {id} not found", 'fileId':id}, 404)
    fileDict = dict(file)

    filename = fileDict['Filename']
    db = get_db()
    cursor = db.execute("SELECT * FROM `Fragments` WHERE `FileID`=?", [id])
    fragments = cursor.fetchall()
    fragmentFiles = []
    receivedFragmentNames = []
    fragmentNames = []
    fragmentNumbers = []

    for fragment in fragments:
        fragmentDict = dict(fragment)
        if fragmentDict['FragmentNumber'] not in fragmentNumbers:
            fragmentNumbers.append(fragmentDict['FragmentNumber'])
            fragmentNames.append(fragmentDict['FragmentName'])
    fragmentNames.sort(key=lambda x: x[-5:])
    getdata_request = messages_pb2.getdata_request()
    getdata_request.fragmentName1 = fragmentNames[0]
    getdata_request.fragmentName2 = fragmentNames[1]
    getdata_request.fragmentName3 = fragmentNames[2]
    getdata_request.fragmentName4 = fragmentNames[3]

    print(f"Sending request for file: {getdata_request.SerializeToString()}")
    data_req_socket.send(getdata_request.SerializeToString())
    
    wrongFragmentsReceived = 0

    while True:
        try:
            receivedFragmentNames.sort(key=lambda x: x[-5:])
            if (receivedFragmentNames == fragmentNames):
                break
            socks = dict(poller.poll(100000))
        except KeyboardInterrupt:
            break

        if response_socket in socks:
            response = messages_pb2.getdata_response()
            response.ParseFromString(response_socket.recv())
            if (response.fragmentName1 != '' and response.fragmentName1 not in receivedFragmentNames):
                print(f"Received: file: {response.fragmentName1}")
                if (response.fragmentName1 in fragmentNames):
                    fragmentFiles.append(response.fragmentData1)
                    receivedFragmentNames.append(response.fragmentName1)
                else:
                    wrongFragmentsReceived += 1
            if (response.fragmentName2 != '' and response.fragmentName2 not in receivedFragmentNames):
                print(f"Received: file: {response.fragmentName2}")
                if (response.fragmentName2 in fragmentNames):
                    fragmentFiles.append(response.fragmentData2)
                    receivedFragmentNames.append(response.fragmentName2)
                else:
                    wrongFragmentsReceived += 1
            if (response.fragmentName3 != '' and response.fragmentName3 not in receivedFragmentNames):
                print(f"Received: file: {response.fragmentName3}")
                if (response.fragmentName3 in fragmentNames):
                    fragmentFiles.append(response.fragmentData3)
                    receivedFragmentNames.append(response.fragmentName3)
                else:
                    wrongFragmentsReceived += 1
            if (response.fragmentName4 != '' and response.fragmentName4 not in receivedFragmentNames):
                print(f"Received: file: {response.fragmentName4}")
                if (response.fragmentName4 in fragmentNames):
                    fragmentFiles.append(response.fragmentData4)
                    receivedFragmentNames.append(response.fragmentName4)
                else:
                    wrongFragmentsReceived += 1
        else:
            break
    combined_filedata = b"".join(fragmentFiles)
    
    if len(combined_filedata) != fileDict['Size']:
        return make_response({"message": "Received file size does not match expected file size", 'expectedSize':fileDict['Size'], 'actualSize':len(combined_filedata), 'receivedFragments':receivedFragmentNames, 'fragmentNames':fragmentNames, 'wrongFragmentsReceived':wrongFragmentsReceived}, 500)

    os.makedirs(os.path.join('downloaded_data/'), exist_ok=True)
    file_path = f"downloaded_data/{filename}"
    with open(file_path, "wb") as file:
        file.write(combined_filedata)

    end_time = time.time()
    execution_time = end_time - start_time
    print(f"Downloaded file: {filename} in time: {execution_time} seconds")
    
    return make_response({'message': 'File downloaded and saved successfully', 'downloadTime': execution_time, 'numberOfFragments':len(fragmentFiles), 'wrongFragmentsReceived':wrongFragmentsReceived}, 200)

@app.route('/file/<string:filename>',  methods=['GET'])
def download_file(filename, id=None):
    start_time = time.time()
    print(f"Downloading file {filename}")
    if (id == None):
        db = get_db()
        cursor = db.execute("SELECT * FROM `Files` WHERE `Filename`=?", [filename])
        if not cursor: 
            return make_response({"message": "Error connecting to the database"}, 500)
    else:
        db = get_db()
        cursor = db.execute("SELECT * FROM `Files` WHERE `FileID`=?", [id])
        if not cursor: 
            return make_response({"message": "Error connecting to the database"}, 500)
        
    file = cursor.fetchone()
    if not file:
        return make_response({"message": f"File {filename} not found"}, 404)
    
    fileDict = dict(file)
    db = get_db()
    cursor = db.execute("SELECT * FROM `Fragments` WHERE `FileID`=?", [fileDict['FileID']])
    fragments = cursor.fetchall()
    fragmentFiles = []
    receivedFragmentNames = []
    fragmentNames = []
    fragmentNumbers = []

    for fragment in fragments:
        fragmentDict = dict(fragment)
        print(fragmentDict)
        if fragmentDict['FragmentNumber'] not in fragmentNumbers:
            fragmentNumbers.append(fragmentDict['FragmentNumber'])
            fragmentNames.append(fragmentDict['FragmentName'])
    fragmentNames.sort(key=lambda x: x[-5:])
    getdata_request = messages_pb2.getdata_request()
    getdata_request.fragmentName1 = fragmentNames[0]
    getdata_request.fragmentName2 = fragmentNames[1]
    getdata_request.fragmentName3 = fragmentNames[2]
    getdata_request.fragmentName4 = fragmentNames[3]

    print(f"Sending request for file: {getdata_request.SerializeToString()}")
    data_req_socket.send(getdata_request.SerializeToString())
    
    wrongFragmentsReceived = 0

    while True:
        try:
            socks = dict(poller.poll(100))
        except KeyboardInterrupt:
            break

        if response_socket in socks:
            response = messages_pb2.getdata_response()
            response.ParseFromString(response_socket.recv())
            if (response.fragmentName1 != '' and response.fragmentName1 not in receivedFragmentNames):
                print(f"Received: file: {response.fragmentName1}")
                if (response.fragmentName1 in fragmentNames):
                    fragmentFiles.append(response.fragmentData1)
                    receivedFragmentNames.append(response.fragmentName1)
            if (response.fragmentName2 != '' and response.fragmentName2 not in receivedFragmentNames):
                print(f"Received: file: {response.fragmentName2}")
                if (response.fragmentName2 in fragmentNames):
                    fragmentFiles.append(response.fragmentData2)
                    receivedFragmentNames.append(response.fragmentName2)
            if (response.fragmentName3 != '' and response.fragmentName3 not in receivedFragmentNames):
                print(f"Received: file: {response.fragmentName3}")
                if (response.fragmentName3 in fragmentNames):
                    fragmentFiles.append(response.fragmentData3)
                    receivedFragmentNames.append(response.fragmentName3)
            if (response.fragmentName4 != '' and response.fragmentName4 not in receivedFragmentNames):
                print(f"Received: file: {response.fragmentName4}")
                if (response.fragmentName4 in fragmentNames):
                    fragmentFiles.append(response.fragmentData4)
                    receivedFragmentNames.append(response.fragmentName4)
        else:
            break
    combined_filedata = b"".join(fragmentFiles)
    
    if len(combined_filedata) != fileDict['Size']:
        return make_response({"message": "Received file size does not match expected file size", 'expectedSize':fileDict['Size'], 'actualSize':len(combined_filedata), 'receivedFragments':receivedFragmentNames, 'fragmentFiles':fragmentFiles, 'fragmentNames':fragmentNames}, 500)

    os.makedirs(os.path.join('downloaded_data/'), exist_ok=True)
    file_path = f"downloaded_data/{filename}"
    with open(file_path, "wb") as file:
        file.write(combined_filedata)

    end_time = time.time()
    execution_time = end_time - start_time
    print(f"Downloaded file: {filename} in time: {execution_time} seconds")
    
    return make_response({'message': 'File downloaded and saved successfully', 'downloadTime': execution_time, 'numberOfFragments':len(fragmentFiles), 'wrongFragmentsReceived':wrongFragmentsReceived}, 200)

# Endpoint for uploading files
# Splits file into 4 equal sized fragments and generates k full replicas on N different nodes
# Implemented a scheme for general k and N
@app.route('/files', methods=['POST'])
def add_files():
    start_time = time.time()
    file = request.files['file']
    file_data = file.read()
    if file_data is None:
        return make_response({'message': 'Missing file parameters'}, 400)

    strategy = request.form.get('strategy')
    if not strategy:
        return make_response({'message': 'Missing strategy parameter'}, 400)
    
    valid_strategies = ['random', 'min_copysets', 'buddy']
    if strategy not in valid_strategies:
        return make_response({'message': 'Invalid strategy parameter'}, 400)

    file_size = len(file_data)
    fragment_size = math.ceil(file_size / 4)
    fragments = []

    for i in range(0, file_size, fragment_size):
        fragments.append(file_data[i:i+fragment_size])

    # Ensure there are enough nodes to form at least one copyset
    if N < k:
        print("Not enough nodes to form separate copysets for all fragments")
        return make_response({'message': 'Not enough nodes to form separate copysets for all fragments'}, 500)

    print(f"Distributing '{file.filename}' across {N} nodes using strategy '{strategy}'")
    storageFailed = False
    fileId = 0

    if strategy == 'random':
        storageFailed, fileId = random_placement(fragments, file_size, file.filename)
    elif strategy == 'min_copysets':
        storageFailed, fileId = min_copysets_placement(fragments, file_size, file.filename)
    elif strategy == 'buddy':
        storageFailed, fileId = buddy_approach(fragments, file_size, file.filename)

    if storageFailed:
        return make_response({'message': 'Storage failed'}, 500)
    end_time = time.time()
    execution_time = end_time - start_time
    return make_response({'message': f'File uploaded successfully with Id: {fileId}', 'fileId':fileId, 'executionTime':execution_time}, 201)
   
def random_placement(fragments, filesize, filename):
    nodes = list(range(N))
    random.shuffle(nodes)
    
    if (N > k*len(fragments)):
        replication_nodes = random.sample(nodes, k*len(fragments))
    else:
        replication_nodes = nodes.copy()
    print("selected nodes: ", replication_nodes)

    db = get_db()
    cursor = db.execute(
        "INSERT INTO `Files`(`Filename`, `Size`, `ContentType`) VALUES (?,?,?)", (filename, filesize, 'binary')
    )
    db.commit()
    fileId = cursor.lastrowid
    fragmentNumber = 0

    for fragment in fragments:
        replication_nodes = random.sample(nodes, k)
        fragmentNumber += 1
        fragmentName = random_string() + f'_fragment{fragmentNumber}'
        for i in range(k):
            if (len(replication_nodes) == 0):
                replication_nodes = nodes.copy()
            node = replication_nodes.pop()
            send_data(node, fragment, fileId, fragmentNumber, fragmentName+".bin")
    return False, fileId


def min_copysets_placement(fragments, filesize, filename):
    nodes = list(range(N))
    replication_groups = [nodes[i:i + k] for i in range(0, len(nodes), k)]
    
    while len(replication_groups) % k > 1:
        replication_groups.pop()
        
    random.shuffle(replication_groups)
    random_group = random.choice(replication_groups)
    print("selected group: ", random_group)

    db = get_db()
    cursor = db.execute(
        "INSERT INTO `Files`(`Filename`, `Size`, `ContentType`) VALUES (?,?,?)", (filename, filesize, 'binary')
    )
    db.commit()
    fileId = cursor.lastrowid
    fragmentNumber = 0

    random_fragment_names = [random_string() for _ in range(4)]
    
    for node in random_group:
        for i, fragment in enumerate(fragments):
            fragmentNumber += 1
            fragmentName = random_fragment_names[i] + f'_fragment{fragmentNumber}'
            send_data(node, fragment, fileId, fragmentNumber, fragmentName+".bin")
        fragmentNumber = 0
    return False, fileId
            

def buddy_approach(fragments, filesize, filename):
    # Generate k full replicas on N different nodes
    # Pick a random group of nodes from listOfGroups
    if (N <= k):
        random_group = list(range(N))
    else:
        random_group = random.choice(listOfGroups)
    print(f"Random group of nodes selected: {random_group}")
    
    db = get_db()
    cursor = db.execute(
        "INSERT INTO `Files`(`Filename`, `Size`, `ContentType`) VALUES (?,?,?)", (filename, filesize, 'binary')
    )
    db.commit()
    fileId = cursor.lastrowid
    fragmentNumber = 0
    
    for fragment in fragments:
        fragmentNumber += 1
        selected_group = random_group.copy()
        fragmentName = random_string() + f'_fragment{fragmentNumber}'
        for _ in range(k):
            if (len(selected_group) == 0):
                selected_group = random_group.copy()
            node = random.choice(selected_group)
            selected_group.remove(node)
            send_data(node, fragment, fileId, fragmentNumber, fragmentName+".bin")
    return False, fileId


def send_data(node_id, filedata, fileId, fragmentNumber, filename="testfile.bin"):
    request = messages_pb2.storedata_request()
    request.filename = filename
    request.filedata = filedata
    
    serialized_request = request.SerializeToString()

    db = get_db()
    db.execute(
        "INSERT INTO `Fragments`(`FileID`, `FragmentName`, `FragmentNumber`, `NodeIDs`) VALUES (?,?,?,?)",
        (fileId, filename, fragmentNumber, node_id)
    )
    db.commit()
    
    sockets[node_id].send(serialized_request)

if __name__ == '__main__':
    # Clean up database before starting
    with app.app_context():
        clear_database()

    # Start heartbeat monitoring in a separate thread
    threading.Thread(target=heartbeat_monitor, daemon=True).start()
    threading.Thread(target=check_heartbeats, daemon=True).start()
    
    app.run(host="localhost", port=5555)