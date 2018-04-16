# graph-db
Implementation of Graph DB engine

## Overall description
We build a graph db engine. In graph there will be _nodes_ and _edges_.

Nodes have many _properties_ (key-value pairs)

Edges connect pairs of nodes, are directional, there can be loops (`x->x`).
Edges can also have _properties_ (key-value pairs).

Property keys are always `TEXT` (`INT`?). Values can be of different types.

Type    | Size (Bytes) | Description
--------|--------------|----------------------------------------------------------
`BOOL`  | 1            | True/False boolean value. 0 for False, else True
`INT`   | 4            | Signed integer number
`UINT`  | 4            | Unsigned integer number (not available for user?)
`FLOAT` | 4            | Single precision floating point number
`TEXT`  | Varying      | Sequence of `CHAR`s long as needed. Like SQL `VARCHAR`

On low level edges do not store attributes in them, but create a special node in which
edge attributes are stored. Thus, an edge is a `from_node_id`, `to_node_id`,
`props_node_id` triple.

### Program

Nodes and Edges should be compared by ids (maybe redefine `__eq__` and `__ne__` in future).
Even better if we use operator `is` (how?).

Nodes are dictionaries:
```
n = {
    'is_edge': False,
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
    'fnid': 120,  # From node id
    'tnid': 122,  # To node id
    'props': {
        'color': 'red'
    }
}
```

### Common abbreviations

Abbreviation | Description
-------------|------------
`n`          | In code represents a Node object. Can be prefixed by something
`nid`        | Node id (see `n`)
`fnid`       | From Node ID (see `nid`). Used when talking about edges
`tnid`       | To Node ID (see `nid`). Used when talking about edges
`enid`       | Edge Properties Node ID (see `nid`). The ID of a node for storing


#### Saving to disk
##### Files
Byte order everywhere is **big-endian** aka **network order**.

There are 3 files: NODES file, NODE\_IDS file, EDGE\_IDS file.

###### NODES file
The most important file where nodes and properties are stored.

NODES contains a `UINT` number `cur_node_addr` which is the address of the free space to write to.
Then there is a sequence of `node` blocks. `node` blocks have this structure:

Size/Type | Short name   | Description
----------|--------------|------------
`UINT`    | `rec_len`    | Record length in bytes
`BOOL`    | `is_edge`    | Is this a node to store edge properties? See EDGES file
`UINT`    | `num_props`  | How many properties are stored in this node?
Depends on `num_props` | `props`      | Sequence of `prop` blocks

`prop` blocks have this structure:

Size/Type | Short name   | Description
----------|--------------|------------
`UINT`    | `key_strlen` | How many characters in this property key
`key_strlen` of `CHAR`s | `key_chars` | Key characters
`INT`     | `val_desc`   | Describes length and type of the value. See below
Depends on `val_len` | `val` | Value of this property

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
Maps node IDs to their address in NODES file

NODE\_IDS contains an `INT` value `cur_node_id` the first free id. After `cur_node_id` there
is a sequence of blocks. Block represents one node and has this structure: 

Size/Type | Short Name  | Description
----------|-------------|------------
`UINT`    | `naddr`     | Pointer to property block in Nodes file
`UINT`    | `edge_from` | First edge from node 
`UINT`    | `edge_to`   | First edge to node

###### EDGE\_IDS file

Edges are directed connections between two nodes. Properties of an edge are stored
in a special _edge node_

EDGES\_IDS contains an `INT` value `cur_eid` the first free id. After `cur_eid` there
is a sequence of `edge` blocks. `edge` blocks have this structure:

Size/Type | Short Name  | Description
----------|-------------|------------
`UINT`    | `from_nid`  | Edge source node ID
`UINT`    | `to_nid`    | Edge destination node ID
`UINT`    | `prev_1`    | Previous relationship ID for the start node
`UINT`    | `next_1`    | Next relationship ID for the start node
`UINT`    | `prev_2`    | Previous relationship ID for the end node
`UINT`    | `next_2`    | Next relationship ID for the end node
`UINT`    | `props_addr`| Pointer to property block in Nodes file



##### Implementation details
Nodes can be created, updated, deleted.

Update operations return possibly a different `id`.

Function                               | Returns | Description
---------------------------------------|---------|-----------------------------------
`create_node(n)`                       | `nid`   | Save node to disk and return it's id
`update_node(nid, n)`                  | `nid2`  | Write new data for node `nid`. `nid2` is the new id for node `nid`
`get_node(nid)`                        | `n`     | Find node by id
`delete_node(nid)`                     | Nothing | Delete node `nid`
`create_edge(from_nid, to_nid, edge_nid)` | `eid`   | Save edge and return it's id
`remove_edge(eid)`                     | Nothing | Delete edge by edge id
`all_node_ids()`                       | `[nid]` | List of all node ids
`edges_from(nid)`                      | `[eid]` | List of all edge ids from node
`edges_to(nid)`                        | `[eid]` | List of all edge ids to node
