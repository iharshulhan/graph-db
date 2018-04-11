from itertools import repeat
import mmap
import struct
from typing import Any, Dict, NewType, Tuple

Node = NewType('Node', Dict)
NodeId = NewType('NodeId', int)
Edge = NewType('Edge', Dict)
EdgeId = NewType('EdgeId', int)
NodeAddress = NewType('NodeAddress', int)
NodeIdAddress = NewType('NodeIdAddress', int)

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

    # For serializing property values
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
            self._node_ids_mmap[0:self.SIZE_UINT] = self._pack_int(1)
            self._node_ids_mmap.flush()
            self._edge_ids_mmap[0:self.SIZE_UINT] = self._pack_int(1)
            self._edge_ids_mmap.flush()
            self._nodes_mmap[0:self.SIZE_UINT] = self._pack_uint(1)
            self._edges_mmap[0:self.SIZE_UINT] = self._pack_uint(1)

    def _pack_node(self, node: Node) -> bytes:
        ans = self._pack_bool(node['is_edge'])
        ans += self._pack_uint(len(node['props']))
        for k, v in node['props'].items():
            ans += self._pack_text(k)
            ans += self._pack_value(v)

        # prepend len of resulting ans
        ans = self._pack_uint(self.SIZE_UINT + len(ans)) + ans
        return ans

    def _unpack_node(self, b: bytes) -> Node:
        cur = 0
        after = self.SIZE_UINT
        rec_len = self._unpack_uint(b[cur:after])
        cur = self.SIZE_UINT  # skip rec_len
        after = self.SIZE_UINT + self.SIZE_BOOL
        is_edge = self._unpack_bool(b[cur:after])
        cur, after = after, after + self.SIZE_UINT
        num_props = self._unpack_uint(b[cur:after])
        ans: Node = Node({})
        ans['is_edge'] = is_edge
        ans['props'] = {}
        for _ in range(num_props):
            cur, after = after, after + self.SIZE_UINT
            k_len = self._unpack_uint(b[cur:after])
            cur, after = after, after + k_len
            k = self._unpack_text_bytes(b[cur:after])
            cur, after = after, after + self.SIZE_INT
            val_desc = self._unpack_int(b[cur:after])
            val: Any = None
            if val_desc >= 0:
                # This is TEXT
                cur, after = after, after + self.SIZE_INT
                val_len = self._unpack_int(b[cur:after])
                cur, after = after, after + val_len
                val = self._unpack_text_bytes(b[cur:after])
            elif val_desc == self.TYPE_BOOL:
                cur, after = after, after + self.SIZE_BOOL
                val = self._unpack_bool(b[cur:after])
            elif val_desc == self.TYPE_FLOAT:
                cur, after = after, after + self.SIZE_FLOAT
                val = self._unpack_float(b[cur:after])
            elif val_desc == self.TYPE_INT:
                cur, after = after, after + self.SIZE_INT
                val = self._unpack_int(b[cur:after])
            elif val_desc == self.TYPE_UINT:
                cur, after = after, after + self.SIZE_UINT
                val = self._unpack_uint(b[cur:after])
            else:
                raise Exception('Unknown val_desc ' + str(val_desc))
            ans['props'][k] = val
        return ans

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

    def _cur_node_addr(self) -> NodeAddress:
        ans, _ = self._uint_from_nodes(NodeAddress(0))
        return NodeAddress(ans)

    def _write_cur_node_addr(self, x: NodeAddress) -> None:
        self._uint_to_nodes(Uint(x), NodeAddress(0))

    def _write_cur_node_id(self, x: NodeId) -> None:
        self._uint_to_node_ids(Uint(x), NodeIdAddress(0))

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

    def _node_to_nodes(self, x: Node, cur: NodeAddress) -> NodeAddress:
        packed = self._pack_node(x)
        m = self._nodes_mmap
        t = NodeAddress(cur + len(packed))
        m[cur:t] = packed
        return t

    def _map_node_id_to_node_addr(self, nid: NodeId,
                                  naddr: NodeAddress) -> None:
        node_id_address = NodeIdAddress(nid * self.SIZE_UINT)
        self._uint_to_node_ids(Uint(naddr), node_id_address)

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
        id_addr = NodeIdAddress(nid * self.SIZE_UINT)
        ans, _ = self._uint_from_node_ids(id_addr)
        return NodeAddress(ans)

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
        _, cur = self._uint_from_nodes(cur)  # skip rec_len
        ans['is_edge'], cur = self._bool_from_nodes(cur)
        ans['props'] = {}
        num_props, cur = self._uint_from_nodes(cur)
        for _ in repeat(None, num_props):
            k, cur = self._text_from_nodes(cur)
            v, cur = self._value_from_nodes(cur)
            ans['props'][k] = v
        return ans

    def get_node(self, nid: NodeId) -> Node:
        node_addr = self._addr_of_node(nid)
        return self._node_from(node_addr)

    def close(self):
        self._nodes_mmap.close()
        self._node_ids_mmap.close()
        self._edges_mmap.close()
        self._edge_ids_file.close()

        self._nodes_file.close()
        self._node_ids_file.close()
        self._edges_file.close()
        self._edge_ids_file.close()

