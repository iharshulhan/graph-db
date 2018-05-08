"""
A graph engine layer to work with graph storage
"""
from typing import Dict, List, Optional, Tuple
from collections import deque

from graph_storage.storage import GraphStorage, NodeId, Node, EdgeId, Edge
from utils import execute_function_in_parallel

visited_nodes = {}  # array to store history between requests


def check_properties(props: Dict, desirable_props: Dict) -> bool:
    """
    Check if properties satisfy desirable ones
    :param props: props to check
    :param desirable_props: props that are desired
    :return: True or Fasle
    """
    if not props:
        return False

    if not desirable_props:
        return True

    for key, value in desirable_props.items():
        if key == 'negative_props':
            if not isinstance(value, List):
                raise Exception('Negative props type should be a list')
            for neg_prop in value:
                if neg_prop in props:
                    return False
            continue
        if key == 'less_props':
            if not isinstance(value, Dict):
                raise Exception('Less props type should be a dict')
            for ls_key, ls_value in value.items():
                if ls_key not in props or props[ls_key] >= ls_value:
                    return False
            continue
        if key == 'less_or_equal_props':
            if not isinstance(value, Dict):
                raise Exception('Less or equal props type should be a dict')
            for ls_eq_key, ls_eq_value in value.items():
                if ls_eq_key not in props or props[ls_eq_key] > ls_eq_value:
                    return False
            continue
        if key == 'greater_props':
            if not isinstance(value, Dict):
                raise Exception('Greater props type should be a dict')
            for gr_key, gr_value in value.items():
                if gr_key not in props or props[gr_key] <= gr_value:
                    return False
            continue

        if key == 'greater_or_equal_props':
            if value is not Dict:
                raise Exception('Greater or equal props type should be a dict')
            for gr_eq_key, gr_eq_value in value.items():
                if gr_eq_key not in props or props[gr_eq_key] <= gr_eq_value:
                    return False
            continue
        if key == 'equal_props':
            if not isinstance(value, Dict):
                raise Exception('Equal props type should be a dict')
            for eq_key, eq_value in value.items():
                if eq_key not in props or props[eq_key] != eq_value:
                    return False
            continue
        if key == 'not_equal_props':
            if not isinstance(value, Dict):
                raise Exception('Not equal props type should be a dict')
            for neq_key, neq_value in value.items():
                if neq_key not in props or props[neq_key] == neq_value:
                    return False
            continue

    return True


def clear_visited_nodes(query_id: int) -> None:
    """
    Release history for a given query id
    :param query_id: an id of a request
    :return: None
    """
    if query_id in visited_nodes:
        visited_nodes.pop(query_id)


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

        return {'node_id': node_id, 'props': node.get('props', None)}

    def delete_node(self, node_id: NodeId) -> None:
        """
        Delete a node from DB
        :param node_id: an id of the node
        :return: None
        """
        return self.storage.delete_node(node_id)

    def check_property_in_node(self, node_id: int, properties: Dict, node: Node = None) -> Optional[Node]:
        """
        Check if a node contains all props specified in the query
        :param node: the node
        :param node_id: an id of the node
        :param properties: a dict containing properties of the node
        :return: Node or None
        """
        if not node:
            node = self.get_node(node_id)
        if not node:
            return None
        if check_properties(node['props'], properties):
            return node
        else:
            return None

    def get_nodes_by_properties(self, properties: Dict, parallel: bool = False) -> List[Node]:
        """
        Find all nodes which have specified properties
        :param parallel: execute function in parallel
        :param properties: a dict containing desirable properties of a node
        :return: a list of nodes
        """

        all_nodes = self.storage.get_node_ids()
        if parallel:
            return execute_function_in_parallel(self.check_property_in_node,
                                                [(node_id, properties) for node_id in all_nodes])
        else:
            nodes = []
            for node_id in all_nodes:
                res = self.check_property_in_node(node_id, properties)
                if res:
                    nodes.append(res)
            return nodes

    def create_edge(self, from_node: NodeId, properties: Dict,
                    to_node: NodeId = None, to_node_remote: str = None,
                    node_properties: Dict = None) -> Optional[EdgeId]:
        """
        Create a new edge in DB
        :param node_properties: properties for a remote node
        :param to_node_remote: an id of a remote node
        :param to_node: an id of a node where the edge ends
        :param from_node: an id of a node from which the edge starts
        :param properties: a dict containing properties of the edge
        :return: an id of the edge
        """
        # Check if nodes exist
        node1 = self.get_node(from_node)
        node2 = None
        if to_node:
            node2 = self.get_node(to_node)

        if not node1:
            return None
        elif to_node_remote:
            to_node = self.create_node({**node_properties, **{'remote_node_id': to_node_remote, 'remote_node': True}})
        if node2 or to_node_remote:
            return self.storage.create_edge(from_node, to_node, self.storage.create_node({'props': properties}))

    def delete_edge(self, edge_id: EdgeId) -> None:
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

        edge_dict = {'id': edge_id, 'props': edge.get('props', None), 'tnid': edge['tnid'], 'fnid': edge['fnid']}
        if from_node:
            edge_dict['from_node'] = self.get_node(edge['fnid'])
        if to_node:
            edge_dict['to_node'] = self.get_node(edge['tnid'])

        return edge_dict

    def get_edges_from(self, node_id: NodeId, properties: Dict = None, parallel: bool = False) -> List[Edge]:
        """
        Find all edges from a node
        :param parallel: execute function in parallel
        :param node_id: an id of the node
        :param properties: a dict containing desirable properties of an edge
        :return: a list of edges
        """
        node = self.get_node(node_id)
        if not node:
            return []
        edges_ids = self.storage.edges_from(node_id)

        if parallel:
            if not properties:
                results = execute_function_in_parallel(self.get_edge,
                                                       [(edges_id, False, True) for edges_id in edges_ids])
            else:
                results = execute_function_in_parallel(self.check_property_in_edge,
                                                       [(edges_id, properties, False, True) for edges_id in edges_ids])
        else:
            results = []
            for edges_id in edges_ids:
                if not properties:
                    res = self.get_edge(edges_id, False, True)
                else:
                    res = self.check_property_in_edge(edges_id, properties, False, True)
                if res:
                    results.append(res)
        return results

    def get_edges_to(self, node_id: NodeId, properties: Dict = None, parallel: bool = False) -> List[Edge]:
        """
        Find all edges to a node
        :param parallel: execute function in parallel
        :param node_id: an id of the node
        :param properties: a dict containing desirable properties of an edge
        :return: a list of edges
        """

        node = self.get_node(node_id)
        if not node:
            return []
        edges_ids = self.storage.edges_to(node_id)
        if parallel:
            if not properties:
                results = execute_function_in_parallel(self.get_edge,
                                                       [(edges_id, False, True) for edges_id in edges_ids])
            else:
                results = execute_function_in_parallel(self.check_property_in_edge,
                                                       [(edges_id, properties, False, True) for edges_id in edges_ids])
        else:
            results = []
            for edges_id in edges_ids:
                if not properties:
                    res = self.get_edge(edges_id, False, True)
                else:
                    res = self.check_property_in_edge(edges_id, properties, False, True)
                if res:
                    results.append(res)
        return results

    def check_property_in_edge(self, edge_id: int, properties: Dict,
                               from_node: bool = False, to_node: bool = False) -> Optional[Edge]:
        """
        Check if an edge contains all props specified in the query
        :param edge_id: a if of an edge
        :param properties: a dict containing desirable properties of an edge
        :param to_node: if True return node object of a to_node
        :param from_node: if True return node object of a from_node
        :return: Node or None
        """
        edge = self.get_edge(edge_id, from_node, to_node)
        if check_properties(edge['props'], properties):
            return edge
        else:
            return None

    def get_edges_by_properties(self, properties: Dict, parallel: bool = False) -> List[Edge]:
        """
        Find all edges which have specified properties
        :param parallel: execute function in parallel
        :param properties: a dict containing desirable properties of an edge
        :return: a list of edges
        """
        all_edges = self.storage.get_edge_ids()
        if parallel:
            return execute_function_in_parallel(self.check_property_in_edge,
                                                [(edge_id, properties) for edge_id in all_edges])

        else:
            edges = []
            for edge_id in all_edges:
                res = self.check_property_in_edge(edge_id, properties)
                if res:
                    edges.append(res)
            return edges

    def find_neighbours(self, node_id: NodeId, hops: int, query_id: int,
                        node_properties: Dict = None,
                        edge_properties: Dict = None) -> Tuple[List[Node], List[Tuple[str, int]]]:
        """
        Find all neighbours for a node within specified number of hops
        :param query_id: an id of a request
        :param hops: a max distance between neighbours
        :param node_id: an id of the node
        :param node_properties: a dict containing desirable properties for nodes
        :param edge_properties: a dict containing desirable properties for edges
        :return: a list of neighbours, a list of nodes that are not present in this DB
        """
        if hops <= 0:
            return [], []
        if query_id not in visited_nodes:
            visited_nodes[query_id] = {}

        visited_nodes[query_id][node_id] = True
        neighbours = [node_id]
        remote_nodes = []
        queue = deque([(node_id, hops)])

        while len(queue) > 0:
            current_node_id, current_hops = queue.pop()
            if current_hops <= 0:
                break

            if edge_properties:
                edges = self.get_edges_from(node_id, properties=edge_properties)
            else:
                edges = self.get_edges_from(node_id)
            for edge in edges:
                new_node_id = edge['tnid']
                new_node = edge['to_node']
                if not new_node:
                    continue
                if not visited_nodes[query_id].get(new_node_id, None):
                    visited_nodes[query_id][new_node_id] = True
                    if node_properties:
                        res = self.check_property_in_node(new_node_id, node_properties, new_node)
                        if not res:
                            continue

                    if new_node['props'].get('remote_node', None) and new_node['props'].get('remote_node_id', None):
                        remote_nodes.append((new_node['props']['remote_node_id'], current_hops - 1))
                    else:
                        neighbours.append(new_node)
                        queue.append((new_node_id, current_hops - 1))

        return neighbours, remote_nodes
