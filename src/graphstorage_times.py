import numpy as np
import cProfile
from graphstorage import GraphStorage

NODES = 100

def put():
    global adj, graph
    empty_props = {'props': {}}
    mat_to_graph = np.zeros(NODES, dtype=int)
    for i in range(NODES):
        mat_to_graph[i] = graph.create_node(empty_props)

    for i in range(NODES):
        for j in range(NODES):
            edge_prop = graph.create_node(empty_props)
            graph.create_edge(mat_to_graph[i], mat_to_graph[j], edge_prop)


graph = None
adj = None


def setup():
    global adj, graph
    edge_prob = 0.005
    adj = np.random.rand(NODES, NODES)
    adj[adj > 1 - edge_prob] = 1
    adj[adj <= 1 - edge_prob] = 0
    print(len(adj[adj == 1]), 'edges,', NODES, 'nodes')
    graph = GraphStorage(True, 'test')


if __name__ == '__main__':
    setup()
    stats = cProfile.run('put()', sort='cumtime')