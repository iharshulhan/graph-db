"""
A Distributed Graph Database Management System
"""
import json
import random
from time import sleep
from typing import List, Dict, Optional

import requests

from distribute_graph_dbms.start_engines import start_engines
from graph_storage.storage import NodeId, Node, Edge
from utils import execute_function_in_parallel

SUCCESS_CODE = 200


class DBMS:
    def __init__(self, workers: List[str]):
        """
        Initialise a DB with a list of graph engine urls
        :param workers: a list of url to connect to graph engines
        :return: None
        """

        self.workers = []

        for worker in workers:
            url = f'{worker}/ping'
            try:
                response = requests.get(url)
                response.raise_for_status()
                if response.status_code == SUCCESS_CODE:
                    self.workers.append(worker)
            except Exception as e:
                print(f'Worker {worker} did not respond. Error: {e}')

        if not self.workers:
            raise Exception('No db engine workers alive!')

    def add_node(self, props: Dict) -> str:
        """
        Add a node to DB
        :param props: a dict containing properties of the node
        :return: an id of the node
        """
        while True:
            worker_id = random.randrange(start=0, stop=len(self.workers) - 1, step=1)
            url = f'{self.workers[worker_id]}/addNode'

            response = requests.post(url, data={'props': json.dumps(props)})
            if response.status_code != SUCCESS_CODE:
                print(f'Could not add node to engine {self.workers[worker_id]}. Response: {response.text}')
            else:
                node_id = response.json().get('node_id')
                return f'{self.workers[worker_id]}${node_id}'

    def get_node(self, node_id_str: str) -> Optional[Node]:
        """
        Get a node from DB
        :param node_id_str: an id of the node
        :return: the node
        """
        split_node_id = node_id_str.split('$')
        worker = split_node_id[0]
        node_id = split_node_id[1]

        if worker not in self.workers:
            return None

        url = f'{worker}/getNode?node_id={node_id}'

        response = requests.get(url)
        if response.status_code != SUCCESS_CODE:
            print(f'Could not get node from engine {worker}. Response: {response.text}')
        else:
            node = response.json().get('node')
            if not node:
                return node
            node['node_id'] = f'{worker}${node["node_id"]}'
            return node

    def delete_node(self, node_id_str: str) -> None:
        """
        Delete a node from DB
        :param node_id_str: an id of the node
        :return: None
        """
        split_node_id = node_id_str.split('$')
        worker = split_node_id[0]
        node_id = split_node_id[1]

        if worker not in self.workers:
            return None

        url = f'{worker}/deleteNode?node_id={node_id}'

        response = requests.delete(url)
        if response.status_code != SUCCESS_CODE:
            print(f'Could not delete node from engine {worker}. Response: {response.text}')

    def add_edge(self, from_node_id_str: str, to_node_id_str: str, props: Dict) -> str:
        """
        Add an edge to DB
        :param to_node_id_str: an id of a to node
        :param from_node_id_str: an id of a from node
        :param props: a dict containing properties of the edge
        :return: an id of the edge
        """

        split_node_id = from_node_id_str.split('$')
        from_worker = split_node_id[0]
        from_node_id = split_node_id[1]

        split_node_id = to_node_id_str.split('$')
        to_worker = split_node_id[0]
        to_node_id = split_node_id[1]

        if from_worker not in self.workers or to_worker not in self.workers:
            return ''
        data = {'props': json.dumps(props), 'from_node': from_node_id}
        url = f'{from_worker}/addEdge'
        if from_worker == to_worker:
            data['to_node'] = to_node_id
        else:
            data['to_node_remote'] = to_node_id_str

        response = requests.post(url, data=data)
        if response.status_code != SUCCESS_CODE:
            print(f'Could not add edge to engine {from_worker}. Response: {response.text}')
        else:
            edge_id = response.json().get('edge_id')
            return f'{from_worker}${edge_id}'

    def change_ids_in_edge(self, edge: Edge, worker: str):
        """
        Add worker to ids
        :param edge: a dict containing information about edge
        :param worker: a worker id
        :return:
        """
        edge['fnid'] = f'{worker}${edge["fnid"]}'
        if 'from_node' in edge:
            edge['from_node']['node_id'] = f'{worker}${edge["from_node"]["node_id"]}'
        edge['id'] = f'{worker}${edge["id"]}'
        if 'to_node' in edge:
            if edge['to_node']['props'].get('remote_node', None):
                edge['to_node'] = self.get_node(edge["to_node"]["props"]["remote_node_id"])
                edge['tnid'] = edge["to_node"]["node_id"]
            else:
                edge['tnid'] = f'{worker}${edge["tnid"]}'
                edge['to_node']['node_id'] = f'{worker}${edge["to_node"]["node_id"]}'
        return edge

    def get_edge(self, edge_id_str: str) -> Optional[Edge]:
        """
        Get an edge from DB
        :param edge_id_str: an id of the edge
        :return: the edge
        """
        split_edge_id = edge_id_str.split('$')
        worker = split_edge_id[0]
        edge_id = split_edge_id[1]

        if worker not in self.workers:
            return None

        url = f'{worker}/getEdge?edge_id={edge_id}'

        response = requests.get(url)
        if response.status_code != SUCCESS_CODE:
            print(f'Could not get edge from engine {worker}. Response: {response.text}')
        else:
            edge = response.json().get('edge')
            if not edge:
                return None

            return self.change_ids_in_edge(edge, worker)

    def delete_edge(self, edge_id_str: str) -> None:
        """
        Delete an edge from DB
        :param edge_id_str: an id of the edge
        :return: None
        """
        split_edge_id = edge_id_str.split('$')
        worker = split_edge_id[0]
        edge_id = split_edge_id[1]

        if worker not in self.workers:
            return None

        url = f'{worker}/deleteEdge?edge_id={edge_id}'

        response = requests.delete(url)
        if response.status_code != SUCCESS_CODE:
            print(f'Could not delete edge from engine {worker}. Response: {response.text}')

    def get_edges_from(self, node_id_str: str, props: Dict = None) -> Optional[Node]:
        """
        Find all edges from a node
        :param node_id_str: an id of the node
        :param props: a dict containing desirable properties of an edge
        :return: a list of edges
        """
        split_node_id = node_id_str.split('$')
        worker = split_node_id[0]
        node_id = split_node_id[1]

        if worker not in self.workers:
            return None

        url = f'{worker}/getEdgesFrom?node_id={node_id}'
        if props:
            url += f'&props={json.dumps(props)}'

        response = requests.get(url)
        if response.status_code != SUCCESS_CODE:
            print(f'Could not get node from engine {worker}. Response: {response.text}')
        else:
            edges = response.json().get('edges')
            edges_modified = []
            if edges:
                for edge in edges:
                    edges_modified.append(self.change_ids_in_edge(edge, worker))
            return edges_modified


    # @app.route('/getEdgesFrom', methods=['GET'])
    # def get_edge_from():
    #     """
    #     Get all edged from a node
    #     :return: a list of edges
    #     """
    #     node_id = flask.request.args.get('node_id', type=int, default=None)
    #     if not node_id:
    #         return flask.make_response('Node id was not provided', BAD_REQUEST_CODE)
    #     edges = graph.get_edges_from(node_id)
    #     return flask.make_response(flask.jsonify({'edges': edges}), SUCCESS_CODE)
    #
    # @app.route('/getEdgesTo', methods=['GET'])
    # def get_edge_to():
    #     """
    #     Get all edged to a node
    #     :return: a list of edges
    #     """
    #     node_id = flask.request.args.get('node_id', type=int, default=None)
    #     if not node_id:
    #         return flask.make_response('Node id was not provided', BAD_REQUEST_CODE)
    #     edges = graph.get_edges_to(node_id)
    #     return flask.make_response(flask.jsonify({'edges': edges}), SUCCESS_CODE)
    #
    # @app.route('/findNodes', methods=['GET'])
    # def find_nodes():
    #     """
    #     Get all nodes matching props
    #     :return: a list of nodes
    #     """
    #
    #     props = json.loads(flask.request.args.get('props', type=str, default='{}'))
    #
    #     nodes = graph.get_nodes_by_properties(props)
    #     return flask.make_response(flask.jsonify({'nodes': nodes}), SUCCESS_CODE)
    #
    # @app.route('/findEdges', methods=['GET'])
    # def find_edges():
    #     """
    #     Get all edges matching props
    #     :return: a list of edges
    #     """
    #
    #     props = json.loads(flask.request.args.get('props', type=str, default='{}'))
    #
    #     nodes = graph.get_edges_by_properties(props)
    #     return flask.make_response(flask.jsonify({'edges': nodes}), SUCCESS_CODE)
    #
    # @app.route('/findNeighbours', methods=['GET'])
    # def find_neighbours():
    #     """
    #     Get all neighbours of a node
    #     :return: a list of neighbours
    #     """
    #
    #     node_id = flask.request.args.get('node_id', type=int, default=None)
    #     hops = flask.request.args.get('hops', type=int, default=0)
    #     query_id = flask.request.args.get('query_id', type=str, default='')
    #     node_props = json.loads(flask.request.args.get('node_props', type=str, default='{}'))
    #     edge_props = json.loads(flask.request.args.get('edge_props', type=str, default='{}'))
    #
    #     if not node_id:
    #         return flask.make_response('Node id was not provided', BAD_REQUEST_CODE)
    #     if not query_id:
    #         return flask.make_response('Query id was not provided', BAD_REQUEST_CODE)
    #     neighbours, remote_nodes = graph.find_neighbours(node_id, hops, query_id, node_props, edge_props)
    #     return flask.make_response(flask.jsonify({'neighbours': neighbours, 'remote_nodes': remote_nodes}),
    #                                SUCCESS_CODE)


if __name__ == '__main__':
    engines = start_engines()
    sleep(2)
    dbms = DBMS(engines)
    print(dbms.get_edges_from('http://localhost:8081$2'))
    print(dbms.get_edges_from('http://localhost:8081$2', {'negative_props': ['loh2']}))
