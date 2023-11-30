import random
import base64
import messages_pb2

def random_placement(fragments, k, N):

    print("Hello from random_placement")
    # Implement the logic for random placement strategy
    pass

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
    for fragment in fragments:
        # Select random replication group for the fragment
        print(f"Sending fragment to replication group {random_group_index}, nodes: {replication_groups[random_group_index]}")
        
        # Send the fragment to each node in the selected replication group
        for node in replication_groups[random_group_index]:
            # Assuming the node index corresponds to the socket index
            socket = sockets[node]
            socket.send_string(f"Fragment to node {node}") #deleted {fragment}
    


def buddy_approach(fragments, k, N):
    print("Hello from buddy_approach")
    # Implement the logic for buddy approach strategy
    pass
