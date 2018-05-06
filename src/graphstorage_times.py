import numpy as np
import cProfile

from src.graphstorage import GraphStorage

NODES = 1000


def put():
    global adj, graph, mat_to_graph
    empty_props = {'props': {}}
    mat_to_graph = np.zeros(NODES, dtype=int)
    for i in range(NODES):
        mat_to_graph[i] = graph.create_node(empty_props)

    for i in range(NODES):
        for j in range(NODES):
            if adj[i][j] == 1:
                edge_prop = graph.create_node(empty_props)
                graph.create_edge(mat_to_graph[i], mat_to_graph[j], edge_prop)


def get():
    global graph, mat_to_graph
    for i in range(NODES):
        u = mat_to_graph[i]
        graph.get_node(u)
        graph.edges_from(u)


def remove():
    global graph, mat_to_graph
    for i in range(NODES):
        u = mat_to_graph[i]
        edges = graph.edges_from(u)
        for e in edges:
            graph.remove_edge(e)
        graph.delete_node(u)


def get_ids():
    global graph
    node_ids = 0
    for _ in graph.get_node_ids():
        node_ids += 1
    print(node_ids, 'node_ids')
    edge_ids = 0
    for _ in graph.get_edge_ids():
        edge_ids += 1
    print(edge_ids, 'edge_ids')

graph = None
adj = None
mat_to_graph = None


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
    print('Profiling put()...')
    cProfile.run('put()', sort='cumtime')
    print('Profiling get_ids()...')
    cProfile.run('get_ids()', sort='cumtime')
    print('Profiling get()...')
    cProfile.run('get()', sort='cumtime')
    print('Profiling remove()...')
    cProfile.run('remove()', sort='cumtime')
