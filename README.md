# graph-db
Implementation of Graph DB engine

## Plan
### Overall description
We build a graph db engine. In graph there will be _edges_ and _nodes_.
Nodes have _attributes_. Edges have _attributes_.

_Attributes_ is a set of key-value pairs.
Keys are always `TEXT` (`INT`?). Values can be of different types.

Type    | Size (Bytes) | Description
--------|--------------|---------------------------------------
`INT`   | 4            | Signed integer numbers
`FLOAT` | 4            | Single precision floating point number
`CHAR`  | 4            | UTF-8 character
`TEXT`  | Varying      | Sequence of `CHAR`s

On low level edges do not store attributes in them, but create a special node in which
edge attributes are stored. Thus, an edge is a `from_node_id`, `to_node_id`, `params_node_id` triple.

### Saving to disk
#### File layout
There are 3 files: EDGES file, NODE\_IDS file, and NODES file.

##### EDGES file
Stores `from_node_id`, `to_node_id`, `params_node_id` triples.

##### NODE\_IDS file
Helps to find the first byte of a node in NODES file i.e. stores mapping `node_id -> address`.

##### NODES file
Stores node attributes. Layout can be like
```
+------------+--------------+-------------+------------------+--------------+-----+
| num_fields  | field_1_type  | field_1_len  | field_1_contents  | field_2_type  | ... 
+------------+--------------+-------------+------------------+--------------+-----+
```

#### File system interface
Function                               | Returns | Description
---------------------------------------|---------|-----------------------------------
`save_node(n)`                         | `nid`   | Save node to disk and return it's id
`find_node(nid)`                        | `n`     | Find node by id
`save_edge(frm_nid, to_nid, edge_nid)` | `eid`   | Save edge and return it's id
`remove_edge(eid)`                     | Nothing | Delete edge by edge id
`all_node_ids()`                       | `[nid]` | List of all node ids
`edges_from(nid)`                      | `[eid]` | List of all edge ids from node
