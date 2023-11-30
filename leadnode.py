from flask import Flask, make_response, request
import random
import base64
import zmq
import time
from strategy import random_placement, min_copysets_placement, buddy_approach
import math

app = Flask(__name__)
context = zmq.Context()

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
sockets = create_sockets(context, base_port, number_of_nodes)

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
    k = 3
    N = 17

    # Call the appropriate function based on the strategy
    if strategy == 'random':
        random_placement(fragments, k, N)
    elif strategy == 'min_copysets':
        min_copysets_placement(fragments, k, N, sockets)
    elif strategy == 'buddy':
        buddy_approach(fragments, k, N)

    return make_response({'message': 'File uploaded successfully'}, 201)
#
   

# Start the Flask app (must be after the endpoint functions) 
host_local_computer = "localhost" # Listen for connections on the local computer
host_local_network = "0.0.0.0" # Listen for connections on the local network
app.run(host=host_local_computer, port=5555)



