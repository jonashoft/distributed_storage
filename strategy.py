import math
import random
import base64
import messages_pb2
import numpy as np
import random

def random_placement(fragments, k, N):
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

def min_copysets_placement(fragments, k, N, sockets):
    # Print fragments
    print(f"Fragments: {fragments}")


    required_nodes = k * len(fragments)

    #print k
    print(f"k: {k}")

    #print N
    print(f"N: {N}")


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
    


def buddy_approach(fragments, k, N, numberOfGroups, sockets):
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