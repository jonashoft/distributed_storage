from flask import Flask, make_response, request
import random
import base64
import zmq
import time
import math

app = Flask(__name__)
context = zmq.Context()

# Replication factor
k = 5

# Number of nodes
N = 100

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
base_port = 5555
sockets = create_sockets(context, base_port, N)

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

    # Test data
    file_data = "abcdefghijklmnopqrstuvwxyz"

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
        storageFailed = random_placement(fragments)
    elif strategy == 'min_copysets':
        storageFailed = min_copysets_placement(fragments)
    elif strategy == 'buddy':
        storageFailed = buddy_approach(fragments, numberOfGroups)

    if storageFailed:
        return make_response({'message': 'Storage failed'}, 500)
    return make_response({'message': 'File uploaded successfully'}, 201)
#
   
def random_placement(fragments):
    # Shuffle the list of nodes
    nodes = list(range(N))
    random.shuffle(nodes)

    # Select k * fragments random nodes for replication
    replication_nodes = random.sample(nodes, k * len(fragments))

    # Send each fragment to k number of nodes
    for fragment in fragments:
        # Send the fragment to k number of nodes
        for i in range(k):
            # Select the next node from the list of replication nodes
            node = replication_nodes.pop()
            print(f"Sending fragment to node {node}: {fragment}")
            # Send the fragment to the node using the appropriate method

def min_copysets_placement(fragments):
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

    # Send all fragments to each node in random_group
    for node in random_group:
        # Send each fragment to each node in the selected replication group
        for fragment in fragments:
            # Assuming the node index corresponds to the socket index
            print(f"Sending fragment to node {node}: {fragment}")

def buddy_approach(fragments, numberOfGroups):
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

    # Send each fragment to k number of nodes in the selected group
    for fragment in fragments:
        selected_group = random_group.copy()
        # Send the fragment to k number of nodes in the selected group
        for i in range(k):
            # Select the next node from the list of nodes in the selected group
            node = random.choice(selected_group)
            selected_group.remove(node)
            print(f"Sending fragment to node {node}: {fragment}")
            # Send the fragment to the node using the appropriate method


# Start the Flask app (must be after the endpoint functions) 
host_local_computer = "localhost" # Listen for connections on the local computer
host_local_network = "0.0.0.0" # Listen for connections on the local network
app.run(host=host_local_computer, port=5555)



