"""
REST-API for GraphEngine
"""
import json

import flask
from flask import Flask

from graph_engine.engine import GraphEngine

app = Flask(__name__)

SUCCESS_CODE = 200
BAD_REQUEST_CODE = 400
graph = None


@app.route('/addNode', methods=['POST'])
def add_node():
    """
    Add a node to DB
    :return: a node id
    """

    props = json.loads(flask.request.form.get('props'))

    if not props:
        return flask.make_response('No data provided', BAD_REQUEST_CODE)
    node_id = graph.create_node(props)
    return flask.make_response(flask.jsonify({'node_id': node_id}), SUCCESS_CODE)


@app.route('/getNode', methods=['GET'])
def get_node():
    """
    Get a node from DB
    :return: a node
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

    props = json.loads(flask.request.form.get('props'))
    from_node_id = flask.request.form.get('from_node', type=int, default=None)
    to_node_id = flask.request.form.get('to_node', type=int, default=None)

    print(props, from_node_id, to_node_id)
    if not (props and from_node_id and to_node_id):
        return flask.make_response('Not enough data was provided', BAD_REQUEST_CODE)
    edge_id = graph.create_edge(from_node_id, to_node_id, props)
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
def get_edge_from():
    """
    Get all edged from a node
    :return: a list of edges
    """
    node_id = flask.request.args.get('node_id', type=int, default=None)
    if not node_id:
        return flask.make_response('Node id was not provided', BAD_REQUEST_CODE)
    edges = graph.get_edges_from(node_id)
    return flask.make_response(flask.jsonify({'edges': edges}), SUCCESS_CODE)


@app.route('/getEdgesTo', methods=['GET'])
def get_edge_to():
    """
    Get all edged to a node
    :return: a list of edges
    """
    node_id = flask.request.args.get('node_id', type=int, default=None)
    if not node_id:
        return flask.make_response('Node id was not provided', BAD_REQUEST_CODE)
    edges = graph.get_edges_to(node_id)
    return flask.make_response(flask.jsonify({'edges': edges}), SUCCESS_CODE)


if __name__ == '__main__':
    port = 8080
    graph_name = 'test'
    graph = GraphEngine(graph_name)
    app.run(host='0.0.0.0', port=port, threaded=True)
