"""
A graph engine layer to work with graph storage
"""
from multiprocessing.pool import ThreadPool, Pool
from typing import Dict, List, Optional, Callable

from graph_storage.storage import GraphStorage, NodeId, Node, EdgeId, Edge

# number of threads should be bigger than actual number of tasks, elsewhere the pool will not work
NUM_OF_THREADS = 150
thread_pool = ThreadPool(NUM_OF_THREADS + 1)

NUM_OF_PROCESSES = 10
process_pool = Pool(NUM_OF_PROCESSES + 1)


def execute_function_in_parallel(func: Callable, list_args: List, processes: bool = False) -> List:
    """
    Execute a function in parallel using ThreadPool or ProcessPool
    :param processes: execute tasks in separate processes
    :param func: a func to call
    :param list_args: an array containing calling params
    :return: an array with results
    """

    results = []
    pool = process_pool if processes else thread_pool
    # ThreadPool works if number of tasks is less than number of threads, so we feed it with batches of tasks
    for i in range((len(list_args) // NUM_OF_THREADS) + 1):  # +1 to cover a remaining part of integer division
        results_tmp = [pool.apply_async(func, args=(*args,))
                       for args in list_args[i * NUM_OF_THREADS: (i + 1) * NUM_OF_THREADS]]
        for result in results_tmp:
            results.append(result.get())
    return results


class GraphEngine:

    def __init__(self, name: str):
        self.storage = GraphStorage(db_name=name)

    def create_node(self, properties: Dict) -> NodeId:
        """
        Create a new node in DB
        :param properties: a dict containing properties of the node
        :return: an id of the node
        """
        return self.storage.create_node({'props': properties})

    def get_node(self, node_id: NodeId) -> Optional[Node]:
        """
        Find a node by its id
        :param node_id: an id of the node
        :return: the node
        """
        node = self.storage.get_node(node_id)
        if not node:
            return node

        node.pop('record_address')
        node.pop('record_length')

        return node

    def delete_node(self, node_id: NodeId) -> None:
        """
        Delete a node from DB
        :param node_id: an id of the node
        :return: None
        """
        return self.storage.delete_node(node_id)

    def get_nodes_by_properties(self, properties: Dict) -> List[Node]:
        """
        Find all nodes which have specified properties
        :param properties: a dict containing desirable properties of a node
        :return: a list of nodes
        """
        # TODO

    def create_edge(self, from_node: NodeId, to_node: NodeId, properties: Dict) -> Optional[EdgeId]:
        """
        Create a new edge in DB
        :param to_node: an id of a node where the edge ends
        :param from_node: an id of a node from which the edge starts
        :param properties: a dict containing properties of the edge
        :return: an id of the edge
        """
        # Check if nodes exist
        node1 = self.get_node(from_node)
        node2 = self.get_node(to_node)

        print(node1, node2)
        if node1 and node2:
            return self.storage.create_edge(from_node, to_node, self.storage.create_node({'props': properties}))
        else:
            return None

    def delete_edge(self, edge_id: NodeId) -> None:
        """
        Delete an edge from DB
        :param edge_id: an id of the edge
        :return: None
        """
        return self.storage.remove_edge(edge_id)

    def get_edge(self, edge_id: EdgeId, from_node: bool = False, to_node: bool = False) -> Edge:
        """
        Find an edge by its id
        :param to_node: if True return node object of a to_node
        :param from_node: if True return node object of a from_node
        :param edge_id: an id of the edge
        :return: the edge
        """
        edge = self.storage.get_edge(edge_id)

        if from_node:
            edge['from_node'] = self.get_node(edge['fnid'])
        if to_node:
            edge['to_node'] = self.get_node(edge['tnid'])
        edge['id'] = edge_id

        return edge

    def get_edges_from(self, node_id: NodeId) -> Node:
        """
        Find all edges from a node
        :param node_id: an id of the node
        :return: a list of edges
        """

        node = self.get_node(node_id)
        if not node:
            return []
        edges_ids = self.storage.edges_from(node_id)
        return execute_function_in_parallel(self.get_edge, [(edges_id, False, True) for edges_id in edges_ids])

    def get_edges_to(self, node_id: NodeId) -> Node:
        """
        Find all edges to a node
        :param node_id: an id of the node
        :return: a list of edges
        """

        node = self.get_node(node_id)
        if not node:
            return []
        edges_ids = self.storage.edges_to(node_id)
        print(edges_ids)
        return execute_function_in_parallel(self.get_edge, [(edges_id, False, True) for edges_id in edges_ids])


# def test():
#     graph = GraphEngine('test')
#     node1 = graph.create_node({'loh1': 'yes'})
#     node2 = graph.create_node({'loh2': 'no'})
#     print(node1, node2)
#     edge = graph.create_edge(node1, node2, {'loh3': 'no'})
#
#     print(graph.get_edges_from(node1))
#
#
# test()
