"""
REST-API for GraphEngine
"""
import json

import flask
from flask import Flask

from graph_engine.engine import GraphEngine, clear_visited_nodes

app = Flask(__name__)

SUCCESS_CODE = 200
BAD_REQUEST_CODE = 400
graph = None


@app.route('/ping', methods=['GET'])
def ping():
    """
    Check if worker is alive
    :return:
    """

    return flask.make_response('I am alive', SUCCESS_CODE)


@app.route('/addNode', methods=['POST'])
def add_node():
    """
    Add a node to DB
    :return: a node id
    """

    props = json.loads(flask.request.form.get('props', type=str, default='{}'))

    if not props:
        return flask.make_response('No data provided', BAD_REQUEST_CODE)
    node_id = graph.create_node(props)
    return flask.make_response(flask.jsonify({'node_id': node_id}), SUCCESS_CODE)


@app.route('/getNode', methods=['GET'])
def get_node():
    """
    Get a node from DB
    :return: the node
    """
    node_id = flask.request.args.get('node_id', type=int, default=None)
    if not node_id:
        return flask.make_response('Node id was not provided', BAD_REQUEST_CODE)
    node = graph.get_node(node_id)
    return flask.make_response(flask.jsonify({'node': node}), SUCCESS_CODE)


@app.route('/deleteNode', methods=['DELETE'])
def delete_node():
    """
    Delete a node from DB
    :return: None
    """
    node_id = flask.request.args.get('node_id', type=int, default=None)
    if not node_id:
        return flask.make_response('Node id was not provided', BAD_REQUEST_CODE)
    graph.delete_node(node_id)
    return flask.make_response(flask.jsonify('Deletion was successful'), SUCCESS_CODE)


@app.route('/addEdge', methods=['POST'])
def add_edge():
    """
    Add an edge to DB
    :return: an edge id
    """

    props = json.loads(flask.request.form.get('props', type=str, default='{}'))
    from_node_id = flask.request.form.get('from_node', type=int, default=None)
    to_node_id = flask.request.form.get('to_node', type=int, default=None)
    to_node_remote = flask.request.form.get('to_node_remote', type=str, default=None)

    if not (props and from_node_id and (to_node_id or to_node_remote)):
        return flask.make_response('Not enough data was provided', BAD_REQUEST_CODE)
    edge_id = graph.create_edge(from_node_id, props, to_node_id, to_node_remote)
    return flask.make_response(flask.jsonify({'edge_id': edge_id}), SUCCESS_CODE)


@app.route('/getEdge', methods=['GET'])
def get_edge():
    """
    Get an edge from DB
    :return: an edge
    """
    edge_id = flask.request.args.get('edge_id', type=int, default=None)
    if not edge_id:
        return flask.make_response('Edge id was not provided', BAD_REQUEST_CODE)
    edge = graph.get_edge(edge_id, True, True)
    return flask.make_response(flask.jsonify({'edge': edge}), SUCCESS_CODE)


@app.route('/deleteEdge', methods=['DELETE'])
def delete_edge():
    """
    Delete an edge from DB
    :return: None
    """
    edge_id = flask.request.args.get('edge_id', type=int, default=None)
    if not edge_id:
        return flask.make_response('Edge id was not provided', BAD_REQUEST_CODE)
    graph.delete_edge(edge_id)
    return flask.make_response(flask.jsonify('Deletion was successful'), SUCCESS_CODE)


@app.route('/getEdgesFrom', methods=['GET'])
def get_edges_from():
    """
    Get all edges from a node
    :return: a list of edges
    """
    node_id = flask.request.args.get('node_id', type=int, default=None)
    props = json.loads(flask.request.args.get('props', type=str, default='{}'))

    if not node_id:
        return flask.make_response('Node id was not provided', BAD_REQUEST_CODE)
    edges = graph.get_edges_from(node_id, props)
    return flask.make_response(flask.jsonify({'edges': edges}), SUCCESS_CODE)


@app.route('/getEdgesTo', methods=['GET'])
def get_edges_to():
    """
    Get all edged to a node
    :return: a list of edges
    """
    node_id = flask.request.args.get('node_id', type=int, default=None)
    props = json.loads(flask.request.args.get('props', type=str, default='{}'))

    if not node_id:
        return flask.make_response('Node id was not provided', BAD_REQUEST_CODE)
    edges = graph.get_edges_to(node_id, props)
    return flask.make_response(flask.jsonify({'edges': edges}), SUCCESS_CODE)


@app.route('/findNodes', methods=['GET'])
def find_nodes():
    """
    Get all nodes matching props
    :return: a list of nodes
    """

    props = json.loads(flask.request.args.get('props', type=str, default='{}'))

    nodes = graph.get_nodes_by_properties(props)
    return flask.make_response(flask.jsonify({'nodes': nodes}), SUCCESS_CODE)


@app.route('/findEdges', methods=['GET'])
def find_edges():
    """
    Get all edges matching props
    :return: a list of edges
    """

    props = json.loads(flask.request.args.get('props', type=str, default='{}'))

    nodes = graph.get_edges_by_properties(props)
    return flask.make_response(flask.jsonify({'edges': nodes}), SUCCESS_CODE)


@app.route('/findNeighbours', methods=['GET'])
def find_neighbours():
    """
    Get all neighbours of a node
    :return: a list of neighbours
    """

    node_id = flask.request.args.get('node_id', type=int, default=None)
    hops = flask.request.args.get('hops', type=int, default=0)
    query_id = flask.request.args.get('query_id', type=str, default='')
    node_props = json.loads(flask.request.args.get('node_props', type=str, default='{}'))
    edge_props = json.loads(flask.request.args.get('edge_props', type=str, default='{}'))

    if not node_id:
        return flask.make_response('Node id was not provided', BAD_REQUEST_CODE)
    if not query_id:
        return flask.make_response('Query id was not provided', BAD_REQUEST_CODE)
    neighbours, remote_nodes = graph.find_neighbours(node_id, hops, query_id, node_props, edge_props)
    return flask.make_response(flask.jsonify({'neighbours': neighbours, 'remote_nodes': remote_nodes}), SUCCESS_CODE)


@app.route('/clearVisitedNodes', methods=['PUT'])
def clear_history():
    """
    Clear visited node for given query id
    :return: None
    """
    query_id = flask.request.args.get('query_id', type=str, default='')

    if not query_id:
        return flask.make_response('Query id was not provided', BAD_REQUEST_CODE)
    clear_visited_nodes(query_id)
    return flask.make_response(flask.jsonify('Success'), SUCCESS_CODE)


def start(port: int, db_name: str):
    """
    Start api server
    :param port: a port for the server
    :param db_name: a name of a database
    :return:
    """
    global graph
    graph = GraphEngine(db_name)
    app.run(host='0.0.0.0', port=port, threaded=True)


if __name__ == '__main__':
    start(port=8080, db_name='test')
