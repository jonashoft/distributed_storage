from flask import Flask, make_response, request
import random
import base64
import zmq
import time
import math

app = Flask(__name__)
context = zmq.Context()

# Replication factor
k = 3

# Number of nodes
N = 17

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
number_of_nodes = 17  # Adjust this based on the number of data nodes
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


    numberOfGroups = 4 # Number of groups to split the nodes into for buddy approach

    # Call the appropriate function based on the strategy
    if strategy == 'random':
        random_placement(fragments)
    elif strategy == 'min_copysets':
        min_copysets_placement(fragments)
    elif strategy == 'buddy':
        buddy_approach(fragments, numberOfGroups)

    return make_response({'message': 'File uploaded successfully'}, 201)
#
   

def random_placement(fragments):
    # Print fragments
    print(f"Fragments: {fragments}")
    print(f"k: {k} \nN: {N}")

    # Ensure there are enough nodes to form at least one copyset
    if N < k * len(fragments):
        print("Not enough nodes to form separate copysets for all fragments")
        return
    
    # Shuffle the list of nodes
    nodes = list(range(N))
    random.shuffle(nodes)

    # Iterate over each fragment
    for fragment in fragments:
        # Select k random nodes for replication
        replication_nodes = random.sample(nodes, k)

        # Send the fragment to each replication node
        for node in replication_nodes:
            print(f"Sending fragment to node {node}: {fragment}")
            # Send the fragment to the node using the appropriate method

def min_copysets_placement(fragments):
    # Print fragments
    print(f"Fragments: {fragments}")
    print(f"k: {k} \nN: {N}")


    required_nodes = k * len(fragments)

     # Ensure there are enough nodes to form at least one copyset
    if N < required_nodes:
        print("Not enough nodes to form separate copysets for all fragments")
        return

    # Create a list of node IDs
    nodes = list(range(N))

    # Determine the number of extra nodes
    extra_nodes = N % k
    print(f"Extra nodes: {extra_nodes}")

    # Create a list of node IDs
    nodes = list(range(N - extra_nodes))
    print(f"Nodes: {nodes}")

    # Initialize the data structure for replication groups
    replication_groups = [nodes[i:i + k] for i in range(0, len(nodes), k)]

    # Distribute the extra nodes among the replication groups
    for i in range(extra_nodes):
        replication_groups[i % len(replication_groups)].append(N - i - 1)

    # Select random replication group for first fragment
    random_group_index = random.randint(0, len(replication_groups) - 1)
    print(f"Start index: {random_group_index}")

    # Iterate over each fragment and send it to a replication group
    for i, fragment in enumerate(fragments):
        # Calculate the replication group index, wrapping around if necessary
        group_index = (random_group_index + i) % len(replication_groups)
        print(f"Sending fragment to replication group {group_index}, nodes: {replication_groups[group_index]}")
        
        # Send the fragment to each node in the selected replication group
        for node in replication_groups[group_index]:
            # Assuming the node index corresponds to the socket index
            socket = sockets[node]
            socket.send_string(f"Fragment to node {node}: {fragment}")
    

def buddy_approach(fragments, numberOfGroups):
    # Print fragments
    print(f"Fragments: {fragments}")
    print(f"k: {k} \nN: {N}")

    # Ensure there are enough nodes to form at least one copyset
    if N < k * len(fragments):
        print("Not enough nodes to form separate copysets for all fragments")
        return
    
    # Create a list of node IDs
    nodes = list(range(N))
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

    for node in random_group:
        # Assuming the node index corresponds to the socket index
        socket = sockets[node]
        socket.send_string(f"Fragment to node {node}: {fragments.pop()}")

# Start the Flask app (must be after the endpoint functions) 
host_local_computer = "localhost" # Listen for connections on the local computer
host_local_network = "0.0.0.0" # Listen for connections on the local network
app.run(host=host_local_computer, port=5555)



