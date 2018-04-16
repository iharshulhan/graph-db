import mmap
import struct
from itertools import repeat
from typing import Any, Dict, NewType, Tuple

Node = NewType('Node', Dict)
NodeId = NewType('NodeId', int)
Edge = NewType('Edge', Dict)
EdgeId = NewType('EdgeId', int)
NodeAddress = NewType('NodeAddress', int)
NodeIdAddress = NewType('NodeIdAddress', int)
EdgeAddress = NewType('NodeAddress', int)
EdgeIdAddress = NewType('NodeIdAddress', int)

Uint = NewType('Uint', int)
Int = NewType('Int', int)
Bool = NewType('Bool', bool)
Text = NewType('Text', str)
Float = NewType('Float', float)


class GraphStorage:

    SIZE_BOOL = 1
    SIZE_INT = 4
    SIZE_UINT = 4
    SIZE_FLOAT = 4

    # val_desc possible values
    TYPE_BOOL = -1
    TYPE_INT = -2
    TYPE_UINT = -3
    TYPE_FLOAT = -4

    FILE_SIZE = 1024 * 1024  # initial file size

    def _make_file(self, filename: str, size: int=FILE_SIZE) -> None:
        with open(filename, 'wb') as f:
            f.write(size * b'\0')

    def __init__(self, new: bool, db_name: str='db') -> None:
        self.TYPE_BOOL_PACKED = self._pack_int(self.TYPE_BOOL)
        self.TYPE_INT_PACKED = self._pack_int(self.TYPE_INT)
        self.TYPE_UINT_PACKED = self._pack_int(self.TYPE_UINT)
        self.TYPE_FLOAT_PACKED = self._pack_int(self.TYPE_FLOAT)

        self._db_name = db_name
        self._nodes_filename = db_name + '.nodes'
        self._node_ids_filename = db_name + '.node_ids'
        self._edges_filename = db_name + '.edges'
        self._edge_ids_filename = db_name + '.edge_ids'

        if new:
            self._make_file(self._nodes_filename)
            self._make_file(self._node_ids_filename)
            self._make_file(self._edges_filename)
            self._make_file(self._edge_ids_filename)

        self._nodes_file = open(self._nodes_filename, 'r+b')
        self._node_ids_file = open(self._node_ids_filename, 'r+b')
        self._edges_file = open(self._edges_filename, 'r+b')
        self._edge_ids_file = open(self._edge_ids_filename, 'r+b')

        self._nodes_mmap = mmap.mmap(self._nodes_file.fileno(), 0)
        self._node_ids_mmap = mmap.mmap(self._node_ids_file.fileno(), 0)
        self._edges_mmap = mmap.mmap(self._edges_file.fileno(), 0)
        self._edge_ids_mmap = mmap.mmap(self._edge_ids_file.fileno(), 0)

        if new:
            self._write_cur_node_addr(NodeAddress(1))
            self._write_cur_node_id(NodeId(1))
            self._write_cur_edge_id(EdgeId(1))

    def _pack_node(self, node: Node) -> bytes:
        ans = self._pack_bool(node['is_edge'])
        ans += self._pack_uint(Uint(len(node['props'])))
        for k, v in node['props'].items():
            ans += self._pack_text(k)
            ans += self._pack_value(v)

        # prepend len of resulting ans
        ans = self._pack_uint(Uint(self.SIZE_UINT + len(ans))) + ans
        return ans

    def _pack_edge(self, fr: NodeId, to: NodeId, prev1: EdgeId,
                   next1: EdgeId, prev2: EdgeId, next2: EdgeId, edg: NodeId) -> bytes:
        ans = self._pack_uint(Uint(fr))
        ans += self._pack_uint(Uint(to))
        ans += self._pack_uint(Uint(prev1))
        ans += self._pack_uint(Uint(next1))
        ans += self._pack_uint(Uint(prev2))
        ans += self._pack_uint(Uint(next2))
        ans += self._pack_uint(self._addr_of_node(edg))

        return ans

    def _set_first_edge_from(self, nid: NodeId, edge: EdgeId) -> None:
        node_id_address = NodeIdAddress(nid * 3 * self.SIZE_UINT)
        self._uint_to_node_ids(Uint(edge), node_id_address + self.SIZE_UINT)

    def _set_first_edge_to(self, nid: NodeId, edge: EdgeId) -> None:
        node_id_address = NodeIdAddress(nid * 3 * self.SIZE_UINT)
        self._uint_to_node_ids(Uint(edge), node_id_address + 2 * self.SIZE_UINT)

    def _pack_value(self, x: Any) -> bytes:
        if type(x) is int:
            return self.TYPE_INT_PACKED + self._pack_int(x)
        elif type(x) is float:
            return self.TYPE_FLOAT_PACKED + self._pack_float(x)
        elif type(x) is str:
            return self._pack_text(x)
        elif type(x) is bool:
            return self.TYPE_BOOL_PACKED + self._pack_bool(x)
        else:
            raise Exception('Can\'t serialize type' + str(type(x)))

    def _pack_float(self, x: float) -> bytes:
        return struct.pack('!f', x)

    def _unpack_float(self, b: bytes) -> Float:
        ans = struct.unpack('!f', b)[0]
        return ans

    def _pack_text(self, x: str) -> bytes:
        encoded = x.encode('utf8', 'strict')
        ans = struct.pack('!I', len(encoded)) + encoded
        return ans

    def _unpack_text_bytes(self, b: bytes) -> Text:
        return Text(b.decode('utf8', 'strict'))

    def _pack_bool(self, x: bool) -> bytes:
        return struct.pack('!?', x)

    def _unpack_bool(self, b: bytes) -> Bool:
        ans = struct.unpack('!?', b)[0]
        return ans

    def _pack_int(self, x: int) -> bytes:
        return struct.pack('!i', x)

    def _pack_uint(self, x: Uint) -> bytes:
        return struct.pack('!I', x)

    def _unpack_int(self, b: bytes) -> Int:
        ans = struct.unpack('!i', b)[0]
        return ans

    def _unpack_uint(self, b: bytes) -> Uint:
        ans = struct.unpack('!I', b)[0]
        return ans

    def _cur_node_id(self) -> NodeId:
        ans, _ = self._uint_from_node_ids(NodeIdAddress(0))
        return NodeId(ans)

    def _cur_edge_id(self) -> EdgeId:
        ans = self._uint_from_edge_ids(EdgeIdAddress(0))
        return EdgeId(ans)

    def _cur_node_addr(self) -> NodeAddress:
        ans, _ = self._uint_from_nodes(NodeAddress(0))
        return NodeAddress(ans)

    # def _cur_edge_addr(self) -> EdgeAddress:
    #     ans, _ = self._uint_from_edges(NodeAddress(0))
    #     return EdgeAddress(ans)

    def _write_cur_node_addr(self, x: NodeAddress) -> None:
        self._uint_to_nodes(Uint(x), NodeAddress(0))

    def _write_cur_node_id(self, x: NodeId) -> None:
        self._uint_to_node_ids(Uint(x), NodeIdAddress(0))

    def _write_cur_edge_id(self, x: EdgeId) -> None:
        self._uint_to_edge_ids(Uint(x), EdgeIdAddress(0))

    def _uint_to_nodes(self, x: Uint, cur: NodeAddress) -> NodeAddress:
        m = self._nodes_mmap
        t = NodeAddress(cur + self.SIZE_UINT)
        m[cur:t] = self._pack_uint(x)
        return t

    def _uint_to_node_ids(self, x: Uint, cur: NodeIdAddress) -> NodeIdAddress:
        m = self._node_ids_mmap
        t = NodeIdAddress(cur + self.SIZE_UINT)
        m[cur:t] = self._pack_uint(x)
        return t

    def _uint_to_edge_ids(self, x: Uint, cur: EdgeIdAddress) -> EdgeIdAddress:
        m = self._edge_ids_mmap
        t = EdgeIdAddress(cur + self.SIZE_UINT)
        m[cur:t] = self._pack_uint(x)
        return t

    def _node_to_nodes(self, x: Node, cur: NodeAddress) -> NodeAddress:
        packed = self._pack_node(x)
        m = self._nodes_mmap
        t = NodeAddress(cur + len(packed))
        m[cur:t] = packed
        return t

    def _edge_to_edges(self, fr: NodeId, to: NodeId, edg: NodeId, cur: EdgeId) -> None:
        if self._first_edge_from_node(fr) == 0:
            self._set_first_edge_from(fr, cur)
            prev1 = EdgeId(0)
        else:
            prev1 = self._last_edge_from(fr)
            self._set_next_edge_from(prev1, cur)
        if self._first_edge_to_node(to) == 0:
            self._set_first_edge_to(to, cur)
            prev2 = EdgeId(0)
        else:
            prev2 = self._last_edge_to(fr)
            self._set_next_edge_to(prev1, cur)
        nxt1 = EdgeId(0)
        nxt2 = EdgeId(0)
        packed = self._pack_edge(fr, to, prev1, nxt1, prev2, nxt2, edg)
        m = self._edge_ids_mmap
        cur = cur * 7 * self.SIZE_UINT
        t = EdgeAddress(cur + len(packed))
        m[cur:t] = packed

    def _set_next_edge_from(self, prev: EdgeId, nxt: EdgeId) -> None:
        edge_id_address = EdgeIdAddress(prev * 7 * self.SIZE_UINT + 3 * self.SIZE_UINT)
        self._uint_to_edge_ids(Uint(nxt), edge_id_address)

    def _set_next_edge_to(self, prev: EdgeId, nxt: EdgeId) -> None:
        edge_id_address = EdgeIdAddress(prev * 7 * self.SIZE_UINT + 5 * self.SIZE_UINT)
        self._uint_to_edge_ids(Uint(nxt), edge_id_address)

    def _set_prev_edge_from(self, prev: EdgeId, nxt: EdgeId) -> None:
        edge_id_address = EdgeIdAddress(nxt * 7 * self.SIZE_UINT + 2 * self.SIZE_UINT)
        self._uint_to_edge_ids(Uint(prev), edge_id_address)

    def _set_prev_edge_to(self, prev: EdgeId, nxt: EdgeId) -> None:
        edge_id_address = EdgeIdAddress(nxt * 7 * self.SIZE_UINT + 4 * self.SIZE_UINT)
        self._uint_to_edge_ids(Uint(prev), edge_id_address)

    def _last_edge_from(self, nid: NodeId) -> EdgeId:
        ans = self._first_edge_from_node(nid)
        while self._next_edge_from(EdgeId(ans)) != 0:
            ans = self._next_edge_from(EdgeId(ans))
        return EdgeId(ans)

    def _last_edge_to(self, nid: NodeId) -> EdgeId:
        ans = self._first_edge_to_node(nid)
        while self._next_edge_to(EdgeId(ans)) != 0:
            ans = self._next_edge_to(EdgeId(ans))
        return EdgeId(ans)

    def _next_edge_to(self, edge: EdgeId) -> EdgeId:
        id_addr = EdgeIdAddress(edge * 7 * self.SIZE_UINT)
        ans = self._uint_from_edge_ids(id_addr + 5 * self.SIZE_UINT)
        return EdgeId(ans)

    def _next_edge_from(self, edge: EdgeId) -> EdgeId:
        id_addr = EdgeIdAddress(edge * 7 * self.SIZE_UINT)
        ans = self._uint_from_edge_ids(id_addr + 3 * self.SIZE_UINT)
        return EdgeId(ans)

    def _prev_edge_to(self, edge: EdgeId) -> EdgeId:
        id_addr = EdgeIdAddress(edge * 7 * self.SIZE_UINT)
        ans = self._uint_from_edge_ids(id_addr + 4 * self.SIZE_UINT)
        return EdgeId(ans)

    def _prev_edge_from(self, edge: EdgeId) -> EdgeId:
        id_addr = EdgeIdAddress(edge * 7 * self.SIZE_UINT)
        ans = self._uint_from_edge_ids(id_addr + 2 * self.SIZE_UINT)
        return EdgeId(ans)

    def _map_node_id_to_node_addr(self, nid: NodeId,
                                  naddr: NodeAddress) -> None:
        node_id_address = NodeIdAddress(nid * 3 * self.SIZE_UINT)
        self._uint_to_node_ids(Uint(naddr), node_id_address)
        self._uint_to_node_ids(Uint(0), node_id_address + self.SIZE_UINT)
        self._uint_to_node_ids(Uint(0), node_id_address + 2 * self.SIZE_UINT)

    def create_node(self, node: Node) -> NodeId:
        cur_id = self._cur_node_id()
        next_id = NodeId(cur_id + 1)
        cur_addr = self._cur_node_addr()
        next_addr = self._node_to_nodes(node, cur_addr)
        self._write_cur_node_addr(next_addr)
        self._write_cur_node_id(next_id)

        self._map_node_id_to_node_addr(cur_id, cur_addr)
        return cur_id

    def _uint_from_node_ids(self, f: NodeIdAddress) -> Tuple[Uint,
                                                             NodeIdAddress]:
        m = self._node_ids_mmap
        t = NodeIdAddress(f + self.SIZE_UINT)
        return (self._unpack_uint(m[f:t]), t)

    def _uint_from_edge_ids(self, f: EdgeIdAddress) -> Uint:
        m = self._edge_ids_mmap
        t = EdgeIdAddress(f + self.SIZE_UINT)
        return self._unpack_uint(m[f:t])

    def _float_from_nodes(self, cur: NodeAddress) -> Tuple[Float, NodeAddress]:
        m = self._nodes_mmap
        t = NodeAddress(cur + self.SIZE_FLOAT)
        return (self._unpack_float(m[cur:t]), t)

    def _uint_from_nodes(self, f: NodeAddress) -> Tuple[Uint, NodeAddress]:
        m = self._nodes_mmap
        t = NodeAddress(f + self.SIZE_UINT)
        return (self._unpack_uint(m[f:t]), t)

    def _int_from_nodes(self, cur: NodeAddress) -> Tuple[Int, NodeAddress]:
        m = self._nodes_mmap
        t = NodeAddress(cur + self.SIZE_INT)
        return (self._unpack_int(m[cur:t]), t)

    def _bool_from_nodes(self, f: NodeAddress) -> Tuple[Bool, NodeAddress]:
        m = self._nodes_mmap
        t = NodeAddress(f + self.SIZE_BOOL)
        return (self._unpack_bool(m[f:t]), t)

    def _text_from_nodes(self, f: NodeAddress) -> Tuple[Text, NodeAddress]:
        length, bytes_f = self._uint_from_nodes(f)
        bytes_t = NodeAddress(bytes_f + length)
        m = self._nodes_mmap
        bts = m[bytes_f:bytes_t]
        return (self._unpack_text_bytes(bts), bytes_t)

    def _addr_of_node(self, nid: NodeId) -> NodeAddress:
        id_addr = NodeIdAddress(nid * 3 * self.SIZE_UINT)
        ans, _ = self._uint_from_node_ids(id_addr)
        return NodeAddress(ans)

    def _first_edge_from_node(self, nid: NodeId) -> EdgeId:
        id_addr = NodeIdAddress(nid * 3 * self.SIZE_UINT + self.SIZE_UINT)
        ans, _ = self._uint_from_node_ids(id_addr)
        return EdgeId(ans)

    def _first_edge_to_node(self, nid: NodeId) -> EdgeId:
        id_addr = NodeIdAddress(nid * 3 * self.SIZE_UINT + 2 * self.SIZE_UINT)
        ans, _ = self._uint_from_node_ids(id_addr)
        return EdgeId(ans)

    def _value_from_nodes(self, cur: NodeAddress) -> Tuple[Any, NodeAddress]:
        m = self._nodes_mmap
        val_desc, cur = self._int_from_nodes(cur)
        if val_desc >= 0:
            t = NodeAddress(cur + val_desc)
            text_bytes = m[cur:t]
            return (self._unpack_text_bytes(text_bytes), t)
        if val_desc == self.TYPE_BOOL:
            return self._bool_from_nodes(cur)
        if val_desc == self.TYPE_INT:
            return self._int_from_nodes(cur)
        if val_desc == self.TYPE_UINT:
            return self._uint_from_nodes(cur)
        if val_desc == self.TYPE_FLOAT:
            return self._float_from_nodes(cur)
        raise Exception('Unknown val_desc ' + str(val_desc))

    def _node_from(self, naddr: NodeAddress) -> Node:
        ans: Node = Node({})
        cur: NodeAddress = naddr
        rec_len, cur = self._uint_from_nodes(cur)
        ans['is_edge'], cur = self._bool_from_nodes(cur)
        ans['node_addr'] = naddr
        ans['rec_len'] = rec_len
        ans['props'] = {}
        num_props, cur = self._uint_from_nodes(cur)
        for _ in repeat(None, num_props):
            k, cur = self._text_from_nodes(cur)
            v, cur = self._value_from_nodes(cur)
            ans['props'][k] = v
        return ans

    def _edge_from(self, eaddr: EdgeAddress) -> Edge:
        ans: Edge = Edge({})
        ans['fnid'] = self._uint_from_edge_ids(eaddr)
        ans['tnid'] = self._uint_from_edge_ids(eaddr + self.SIZE_UINT)
        node: Node = self._node_from(self._uint_from_edge_ids(eaddr + 6 * self.SIZE_UINT))
        ans['props'] = node['props']
        return ans

    def get_node(self, nid: NodeId) -> Node:
        node_addr = self._addr_of_node(nid)
        if node_addr == 0:
            return None
        return self._node_from(node_addr)

    def get_edge(self, eid: EdgeId) -> Edge:
        if self._uint_from_edge_ids(eid * 7 * self.SIZE_UINT) == 0:
            return None
        return self._edge_from(eid * 7 * self.SIZE_UINT)

    def _packed_node_to_nodes(self, packed: bytes,
                              cur: NodeAddress) -> NodeAddress:
        m = self._nodes_mmap
        t = NodeAddress(cur + len(packed))
        m[cur:t] = packed
        return t

    def update_node(self, nid: NodeId, n: Node) -> None:
        old_addr = self._addr_of_node(nid)
        node = self._node_from(old_addr)
        if node is None:
            raise Exception('Updating a non-existing node nid=' + str(nid))
        packed = self._pack_node(n)
        new_rec_len = len(packed)
        old_rec_len = node['rec_len']
        if new_rec_len == old_rec_len:
            self._packed_node_to_nodes(packed, old_addr)
        else:
            cur_node = self._cur_node_addr()
            cur_next_node = self._packed_node_to_nodes(packed, cur_node)
            self._write_cur_node_addr(cur_next_node)

    def delete_node(self, nid: NodeId) -> None:
        self._map_node_id_to_node_addr(nid, NodeAddress(0))

    def create_edge(self, from_nid: NodeId, to_nid: NodeId, edge_nid: NodeId) -> EdgeId:
        cur_id = self._cur_edge_id()
        next_id = EdgeId(cur_id + 1)
        self._edge_to_edges(from_nid, to_nid, edge_nid, cur_id)
        self._write_cur_edge_id(next_id)
        return cur_id

    def remove_edge(self, eid: EdgeId) -> None:
        eaddr = eid * 7 * self.SIZE_UINT
        fr = self._uint_from_edge_ids(eaddr)
        to = self._uint_from_edge_ids(eaddr + self.SIZE_UINT)
        if self._first_edge_from_node(fr) == eid:
            nxt = self._next_edge_from(eid)
            self._set_first_edge_from(fr, nxt)
            self._set_prev_edge_from(EdgeId(0), nxt)
        else:
            nxt = self._next_edge_from(eid)
            prev = self._prev_edge_from(eid)
            self._set_prev_edge_from(prev, nxt)
            self._set_next_edge_from(prev, nxt)
        if self._first_edge_to_node(to) == eid:
            nxt = self._next_edge_to(eid)
            self._set_first_edge_to(to, nxt)
            self._set_prev_edge_to(EdgeId(0), nxt)
        else:
            nxt = self._next_edge_to(eid)
            prev = self._prev_edge_to(eid)
            self._set_prev_edge_to(prev, nxt)
            self._set_next_edge_to(prev, nxt)
        return None

    def edges_from(self, nid: NodeId):
        edges = []
        f = self._first_edge_from_node(nid)
        if f != 0:
            edges.append(f)
            s = self._next_edge_from(f)
            while s != 0:
                edges.append(s)
                s = self._next_edge_from(s)
        return edges

    def edges_to(self, nid: NodeId):
        edges = []
        f = self._first_edge_to_node(nid)
        if f != 0:
            edges.append(f)
            s = self._next_edge_to(f)
            while s != 0:
                edges.append(s)
                s = self._next_edge_to(s)
        return edges

    def close(self):
        self._nodes_mmap.close()
        self._node_ids_mmap.close()
        self._edges_mmap.close()
        self._edge_ids_file.close()

        self._nodes_file.close()
        self._node_ids_file.close()
        self._edges_file.close()
        self._edge_ids_file.close()

