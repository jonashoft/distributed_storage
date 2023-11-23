from flask import Flask, make_response, request
import random
import base64
import zmq
from strategy import random_placement, min_copysets_placement, buddy_approach

context = zmq.Context()
socket = context.socket(zmq.PUSH)
socket.bind("tcp://*:5556")  # Bind to port 5555


app = Flask(__name__)

@app.route('/')
def hello():
    return make_response({'message': 'Namenode'})
#

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
    fragment_size = int(file_size / 4)
    fragments = []

    for i in range(0, file_size, fragment_size):
        fragments.append(file_data[i:i+fragment_size])
    
    # Generate k full replicas on N different nodes
    k = 3
    N = 5

    # Call the appropriate function based on the strategy
    if strategy == 'random':
        random_placement(fragments, k, N)
    elif strategy == 'min_copysets':
        min_copysets_placement(fragments, k, N)
    elif strategy == 'buddy':
        buddy_approach(fragments, k, N)

    return make_response({'message': 'File uploaded successfully'}, 201)
#
   

# Start the Flask app (must be after the endpoint functions) 
host_local_computer = "localhost" # Listen for connections on the local computer
host_local_network = "0.0.0.0" # Listen for connections on the local network
app.run(host=host_local_computer, port=5555)



