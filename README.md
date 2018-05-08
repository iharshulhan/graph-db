# graph-db
Implementation of Graph DB engine

## Installation
### Dependencies
Python 3, Flask.

Pipfile is included. If using Pipenv, installing dependencies can be done by:
```
$ pipenv install
```

### Running
Graph-db can be used on different levels: simple file data storage, single server API, distributed API.

Simple data storage can be used through a class `GraphStorage`. For examples see
or run `graph_storage/graph_storage_test.py` or `graph_storage/graph_storage_times.py`. To run tests:
```
$ python3 graph_storage/graph_storage_tests.py
```

To run a single server Flask API process:
```
$ python3 graph_engine/api.py
```

To run a distributed graph DBMS:
```
$ python3 distribute_graph_dbms/dbms.py
```

There is also a prepared demo on a sample [dataset](https://snap.stanford.edu/data/egonets-Facebook.html)
with distributed database:
```
$ python3 graph_demo/graph_demo.py
```

### Usage
In order to manage a database users can use GraphEngine class directly without REST-API. This is the fastest way. However,
if distributed database management is the case, the user should use distributed DBMS class which is based on REST queries
to graph engine.
#### Graph engine
To create a database, create an instance of GraphEngine object with a specified name of a database.
To manage a database following functions are available.

Name|Description
----|-----------
`create_node(properties)` | Create a new node in DB. _Properties_: a dictionary containing properties of the node. Returns an id of the node
`create_edge(from_node_id, properties, to_node_id)` | Create a new edge in DB. Returns an ID of an edge
`get_node(node_id)` | Find a node by its id
`get_edge(edge_id, from_node: bool, to_node: bool)` | Find an edge by its id. If optional parameter _to_node_ is True return node object of a to_node. If optional parameter _from_node_ is True return node object of a from_node. Returns an edge
`delete_node(node_id)` | Delete a node from DB
`delete_edge(edge_id)` | Delete an edge from DB
`check_property_in_node(node_id, properties, node)` | Check if a node contains all props specified in the query. _Node_ parameter is optional
`check_property_in_edge(edge_id, properties, from_node: bool, to_node: bool)` | Check if an edge contains all properties specified in the query. If optional parameter _to_node_ is True return node object of a to_node. If optional parameter _from_node_ is True return node object of a from_node.
`get_nodes_by_properties(properties)` | Find all nodes which have specified properties. Returns list of nodes
`get_edges_by_properties(properties)` | Find all edges which have specified properties. Returns list of edges
`get_edges_from(node_id, properties)` | Find all edges from a node. Parameter _properties_ is optional. Returns list of edges
`get_edges_to(node_id, properties)` | Find all edges to a node. Parameter _properties_ is optional. Returns list of edges
`find_neighbours(node_id, hops, query_id, node_properties, edge_properties)` | Find all neighbours for a node within specified number of hops. _Query_id_ is an id of a request. _Hops_ is a max distance between neighbours. _Node_properties_ is an optional parameter with a dictionary containing desirable properties for nodes._Edge_properties_ is an optional parameter with a dictionary containing desirable properties for edges.

#### Distributed DBMS
To create a distributed database, create an instance of DBMS object with a specified urls of graph engines. To manage 
a database following functions are available.

Name | Description
-----|------------
`add_node(props)` | Create a new node in DB. _Props_: a dictionary containing properties of the node. Returns an id of the node
`get_node(node_id_str)` | Find a node by its id
`delete_node(node_id_str)` | Delete a node from DB
`add_edge(from_node_id_str, to_node_id_str, props)` | Create a new edge in DB. Returns an ID of an edge
`get_edge(edge_id_str)` | Find an edge by its id. Returns an edge if found
`delete_edge(edge_id_str)` | Delete an edge from DB
`get_edges_from(node_id_str, props)` | Find all edges from a node. Parameter _props_ is optional. Returns list of edges
`get_edges_to(node_id_str, props)` | Find all edges to a node. Parameter _props_ is optional. Returns list of edges
`find_nodes(props)` | Find all nodes which have specified properties. Parameter _props_ is optional. _Props_ is a dictionary containing desirable properties of a node. Returns list of nodes
`find_edges(props)` | Find all edges which have specified properties. Parameter _props_ is optional. _Props_ is a dictionary containing desirable properties of a edges. Returns list of edges
`find_neighbours(node_id_str, hops, node_props, edge_props)` | Find all neighbours for a node within specified number of hops. _Hops_ is a max distance between neighbours. _Node_props_ is an optional parameter with a dictionary containing desirable properties for nodes. _Edge_props_ is an optional parameter with a dictionary containing desirable properties for edges.

## I/O description
Nodes have many _properties_ (key-value pairs)

Edges connect pairs of nodes, are directional, there can be loops (`x->x`).
Edges can also have _properties_ (key-value pairs).

Property keys are always `TEXT`. Values can be of different types.

Type    | Size (Bytes) | Description
--------|--------------|----------------------------------------------------------
`BOOL`  | 1            | True/False boolean value. 0 for False, else True
`INT`   | 4            | Signed integer number
`UINT`  | 4            | Unsigned integer number (not available for user)
`FLOAT` | 4            | Single precision floating point number
`TEXT`  | Varying      | Sequence of `CHAR`s long as needed. Like SQL `VARCHAR`

On low level edges do not store attributes in them, but create a special node in which
edge attributes are stored. Thus, an edge is a `from_node_id`, `to_node_id`,
`props_node_id` triple.

### Program
Nodes are dictionaries:
```
n = {
    'id': 123,
    'props': {
        'name': 'vasea',
        'country': 'UA'
    }
}
```

Edges are dictionaries:
```
e = {
    'fnid': 120,  # This edge is from node 120
    'tnid': 122,  # This edge is to node 122
    'props': {
        'color': 'red'
    }
}
```

#### Saving to disk
##### Files
Byte order everywhere is **big-endian** aka **network order**.

There are 3 files: PROPERTIES file, NODE\_IDS file, EDGES file.

###### PROPERTIES file
The most important file where nodes and properties are stored.

PROPERTIES file contains a `UINT` number `cur_node_addr` which is the address of the free space to write to.
Then there is a sequence of `node` blocks. `node` blocks have this structure:

Size/Type | Short name   | Description
----------|--------------|------------
`UINT`    | `rec_len`    | Record length in bytes
`UINT`    | `num_props`  | How many properties are stored in this node
Depends on `num_props` | `props`      | Sequence of `prop` blocks

`prop` blocks have this structure:

Size/Type | Short name   | Description
----------|--------------|------------
`UINT`    | `key_strlen` | How many characters in this property key
`key_strlen` of `CHAR`s | `key_chars` | Key characters
`INT`     | `val_desc`   | Describes length and type of the value. See below
Depends on `val_desc` | `val` | Value of this property

`val_desc` describes type and length of a value.

`val_desc` value | Type
-----------------|----------------------------
Any non-negative | `TEXT` of length `val_desc`
-1               | `BOOL`
-2               | `INT` 
-3               | `UINT`
-4               | `FLOAT`
-5               | `CHAR`

###### NODE\_IDS file
Maps node IDs to addresses in PROPERTIES file

NODE\_IDS contains an `INT` value `cur_node_id` the first free id. After `cur_node_id` there
is a sequence of blocks. Block represents one node and has this structure: 

Size/Type | Short Name  | Description
----------|-------------|------------
`UINT`    | `addr`      | Pointer to the property block in PROPERTIES file or 0 if this node is deleted
`UINT`    | `edge_from` | First edge from node 
`UINT`    | `edge_to`   | First edge to node

###### EDGES file

Edges are directed connections between two nodes. Properties of edges are stored
in EDGES file.

EDGES file contains an `INT` value `cur_eid` the first free id. After `cur_eid` there
is a sequence of `edge` blocks. `edge` blocks have this structure:

Size/Type | Short Name  | Description
----------|-------------|------------
`UINT`    | `from_nid`  | Edge source node ID or 0 if this edge is deleted
`UINT`    | `to_nid`    | Edge destination node ID
`UINT`    | `prev_1`    | Previous edge ID for the start node
`UINT`    | `next_1`    | Next edge ID for the start node
`UINT`    | `prev_2`    | Previous edge ID for the end node
`UINT`    | `next_2`    | Next edge ID for the end node
`UINT`    | `props_addr`| Pointer to property block in PROPERTIES file
