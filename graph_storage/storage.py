import mmap
import struct
from itertools import repeat
from typing import Any, Dict, NewType, Tuple, Optional, Iterable, BinaryIO

import os

Node = NewType('Node', Dict)
NodeId = NewType('NodeId', int)
Edge = NewType('Edge', Dict)
EdgeId = NewType('EdgeId', int)
PropertyAddress = NewType('PropertyAddress', int)
NodeIdAddress = NewType('NodeIdAddress', int)
EdgeAddress = NewType('EdgeAddress', int)

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

    # See _map_node_id_to_node_addr
    SIZE_NODE_ID = 3 * SIZE_UINT

    # See _
    SIZE_EDGE_ID = 7 * SIZE_UINT

    # val_desc possible values
    TYPE_BOOL = -1
    TYPE_INT = -2
    TYPE_UINT = -3
    TYPE_FLOAT = -4

    FILE_SIZE = 1024 * 1024  # initial file size

    NONE = 0
    EDGE_ID_NONE = EdgeId(NONE)
    NODE_ID_NONE = NodeId(NONE)
    PROPERTY_ADDRESS_NONE = PropertyAddress(NONE)

    @staticmethod
    def _make_file(filename: str, size: int = FILE_SIZE) -> None:
        with open(filename, 'wb') as f:
            f.write(size * b'\0')

    def __init__(self, new: bool = False, db_name: str = 'db') -> None:
        """
        Open and load database files

        Parameters
        ----------
        new : bool
            If the files do not exist yet, should be True.
            If set to True but files already exist, files will be
            overwritten
        db_name : str
            Common prefix of the database file names.
        """
        self.TYPE_BOOL_PACKED = self._pack_int(self.TYPE_BOOL)
        self.TYPE_INT_PACKED = self._pack_int(self.TYPE_INT)
        self.TYPE_UINT_PACKED = self._pack_int(self.TYPE_UINT)
        self.TYPE_FLOAT_PACKED = self._pack_int(self.TYPE_FLOAT)

        self._properties_filename = db_name + '.properties'
        self._node_ids_filename = db_name + '.node_ids'
        self._edges_filename = db_name + '.edges'

        check_files = False if os.path.isfile(self._properties_filename) else True
        if new or check_files:
            self._make_file(self._properties_filename)
            self._make_file(self._node_ids_filename)
            self._make_file(self._edges_filename)

        self._properties_file = open(self._properties_filename, 'r+b')
        self._node_ids_file = open(self._node_ids_filename, 'r+b')
        self._edges_file = open(self._edges_filename, 'r+b')

        self._properties_mmap = mmap.mmap(self._properties_file.fileno(), 0)
        self._node_ids_mmap = mmap.mmap(self._node_ids_file.fileno(), 0)
        self._edges_mmap = mmap.mmap(self._edges_file.fileno(), 0)

        if new:
            first_id = 1
            self._write_cur_addr(PropertyAddress(first_id))
            self._write_cur_node_id(NodeId(first_id))
            self._write_cur_edge_id(EdgeId(first_id))

    def _pack_node(self, node: Node) -> bytes:
        ans = self._pack_uint(Uint(len(node['props'])))
        for k, v in node['props'].items():
            ans += self._pack_text(k)
            ans += self._pack_value(v)

        # prepend len of resulting ans
        ans = self._pack_uint(Uint(self.SIZE_UINT + len(ans))) + ans
        return ans

    def _pack_edge_id(self, fr: NodeId, to: NodeId,
                      prev1: EdgeId, next1: EdgeId,
                      prev2: EdgeId, next2: EdgeId, edg: NodeId) -> bytes:
        ans = self._pack_uint(Uint(fr))
        ans += self._pack_uint(Uint(to))
        ans += self._pack_uint(Uint(prev1))
        ans += self._pack_uint(Uint(next1))
        ans += self._pack_uint(Uint(prev2))
        ans += self._pack_uint(Uint(next2))
        ans += self._pack_uint(Uint(self._node_address(edg)))

        assert len(ans) == self.SIZE_EDGE_ID
        return ans

    def _set_first_edge_from(self, nid: NodeId, edge: EdgeId) -> None:
        node_id_address = self._node_id_address(nid)
        self._write_uint_to_node_ids(Uint(edge), NodeIdAddress(node_id_address + self.SIZE_UINT))

    def _set_first_edge_to(self, nid: NodeId, edge: EdgeId) -> None:
        node_id_address = self._node_id_address(nid)
        self._write_uint_to_node_ids(
            Uint(edge), NodeIdAddress(node_id_address + 2 * self.SIZE_UINT))

    def _pack_value(self, x: Any) -> bytes:
        """
        Serialize value into the corresponding db storage format

        Parameters
        ----------
        x : int or float or str or bool
            Value to pack

        Returns
        -------
        Value in db format. Usually it is val_desc + value bytes
        """
        if type(x) is int:
            return self.TYPE_INT_PACKED + self._pack_int(x)
        elif type(x) is float:
            return self.TYPE_FLOAT_PACKED + self._pack_float(x)
        elif type(x) is str:
            return self._pack_text(x)
        elif type(x) is bool:
            return self.TYPE_BOOL_PACKED + self._pack_bool(x)
        else:
            raise Exception("Can't pack type" + str(type(x)))

    @staticmethod
    def _pack_float(x: float) -> bytes:
        return struct.pack('!f', x)

    @staticmethod
    def _unpack_float(b: bytes) -> Float:
        return struct.unpack('!f', b)[0]

    @staticmethod
    def _pack_text(x: str) -> bytes:
        encoded = x.encode('utf8', 'strict')
        return struct.pack('!I', len(encoded)) + encoded

    @staticmethod
    def _unpack_text_bytes(b: bytes) -> Text:
        return Text(b.decode('utf8', 'strict'))

    @staticmethod
    def _pack_bool(x: bool) -> bytes:
        return struct.pack('!?', x)

    @staticmethod
    def _unpack_bool(b: bytes) -> Bool:
        return struct.unpack('!?', b)[0]

    @staticmethod
    def _pack_int(x: int) -> bytes:
        return struct.pack('!i', x)

    @staticmethod
    def _unpack_int(b: bytes) -> Int:
        return struct.unpack('!i', b)[0]

    @staticmethod
    def _pack_uint(x: Uint) -> bytes:
        return struct.pack('!I', x)

    @staticmethod
    def _unpack_uint(b: bytes) -> Uint:
        return struct.unpack('!I', b)[0]

    def _read_cur_node_id(self) -> NodeId:
        ans, _ = self._read_uint_from_node_ids(NodeIdAddress(0))
        return NodeId(ans)

    def _read_cur_edge_id(self) -> EdgeId:
        ans, _ = self._read_uint_from_edges(EdgeAddress(0))
        return EdgeId(ans)

    def _read_cur_addr(self) -> PropertyAddress:
        """
        Get current free address in Properties file

        Returns
        -------
        Current free address in Properties file
        """
        ans, _ = self._read_uint_from_properties(PropertyAddress(0))
        return PropertyAddress(ans)

    def _write_cur_addr(self, x: PropertyAddress) -> None:
        self._write_uint_to_properties(Uint(x), PropertyAddress(0))

    def _write_cur_node_id(self, x: NodeId) -> None:
        self._write_uint_to_node_ids(Uint(x), NodeIdAddress(0))

    def _write_cur_edge_id(self, x: EdgeId) -> None:
        self._write_uint_to_edges(Uint(x), EdgeAddress(0))

    @staticmethod
    def _ensure_length(t: int, file: BinaryIO, file_mmap: mmap) -> mmap:
        if len(file_mmap) > t:
            return file_mmap
        size = len(file_mmap)
        add_size = (t - size) * 2 + int(size / 2)

        file_mmap.flush()
        file_mmap.close()

        file.seek(size)
        file.write(b'\0' * add_size)
        file.flush()

        ans = mmap.mmap(file.fileno(), 0)
        assert(len(ans) > t)

        return ans

    def _ensure_properties_mmap_length(self, t: PropertyAddress) -> mmap:
        self._properties_mmap = self._ensure_length(t, self._properties_file, self._properties_mmap)
        return self._properties_mmap

    def _ensure_node_ids_mmap_length(self, t: NodeIdAddress) -> mmap:
        self._node_ids_mmap = self._ensure_length(t, self._node_ids_file, self._node_ids_mmap)
        return self._node_ids_mmap

    def _ensure_edges_mmap_length(self, t: EdgeAddress) -> mmap:
        self._edges_mmap = self._ensure_length(t, self._edges_file, self._edges_mmap)
        return self._edges_mmap

    def _write_uint_to_properties(self, x: Uint, cur: PropertyAddress) -> PropertyAddress:
        t = PropertyAddress(cur + self.SIZE_UINT)
        m = self._ensure_properties_mmap_length(t)
        m[cur:t] = self._pack_uint(x)
        return t

    def _write_uint_to_node_ids(self, x: Uint,
                                cur: NodeIdAddress) -> NodeIdAddress:
        t = NodeIdAddress(cur + self.SIZE_UINT)
        m = self._ensure_node_ids_mmap_length(t)
        m[cur:t] = self._pack_uint(x)
        return t

    def _write_uint_to_edges(self, x: Uint,
                             cur: EdgeAddress) -> EdgeAddress:
        t = EdgeAddress(cur + self.SIZE_UINT)
        m = self._ensure_edges_mmap_length(t)
        m[cur:t] = self._pack_uint(x)
        return t

    def _write_node_to_properties(self, x: Node, cur: PropertyAddress) -> PropertyAddress:
        packed = self._pack_node(x)
        t = PropertyAddress(cur + len(packed))
        m = self._ensure_properties_mmap_length(t)
        m[cur:t] = packed
        return t

    def _write_edge_to_edges(self, fr: NodeId, to: NodeId,
                             edg: NodeId, cur: EdgeId) -> None:
        first_from = self._read_first_edge_from_node(fr)
        if first_from == self.EDGE_ID_NONE:
            next1 = self.EDGE_ID_NONE
        else:
            self._set_prev_edge_from(cur, first_from)
            next1 = first_from
        self._set_first_edge_from(fr, cur)

        first_to = self._read_first_edge_to_node(to)
        if first_to == self.EDGE_ID_NONE:
            next2 = self.EDGE_ID_NONE
        else:
            self._set_prev_edge_to(cur, first_to)
            next2 = first_to
        self._set_first_edge_to(to, cur)
        prev1 = self.EDGE_ID_NONE
        prev2 = self.EDGE_ID_NONE
        packed = self._pack_edge_id(fr, to, prev1, next1, prev2, next2, edg)
        cur = self._edge_address(cur)
        t = EdgeAddress(cur + len(packed))
        m = self._ensure_edges_mmap_length(t)
        m[cur:t] = packed

    def _set_next_edge_from(self, prev: EdgeId, nxt: EdgeId) -> None:
        edge_id_address = EdgeAddress(self._edge_address(prev) + 3 * self.SIZE_UINT)
        self._write_uint_to_edges(Uint(nxt), edge_id_address)

    def _set_next_edge_to(self, prev: EdgeId, nxt: EdgeId) -> None:
        edge_id_address = EdgeAddress(self._edge_address(prev) + 5 * self.SIZE_UINT)
        self._write_uint_to_edges(Uint(nxt), edge_id_address)

    def _set_prev_edge_from(self, prev: EdgeId, nxt: EdgeId) -> None:
        edge_id_address = EdgeAddress(self._edge_address(nxt) + 2 * self.SIZE_UINT)
        self._write_uint_to_edges(Uint(prev), edge_id_address)

    def _set_prev_edge_to(self, prev: EdgeId, nxt: EdgeId) -> None:
        edge_id_address = EdgeAddress(self._edge_address(nxt) + 4 * self.SIZE_UINT)
        self._write_uint_to_edges(Uint(prev), edge_id_address)

    def _last_edge_from(self, nid: NodeId) -> EdgeId:
        ans = self._read_first_edge_from_node(nid)
        while self._next_edge_from(EdgeId(ans)) != self.EDGE_ID_NONE:
            ans = self._next_edge_from(EdgeId(ans))
        return EdgeId(ans)

    def _last_edge_to(self, nid: NodeId) -> EdgeId:
        ans = self._read_first_edge_to_node(nid)
        while self._next_edge_to(EdgeId(ans)) != self.EDGE_ID_NONE:
            ans = self._next_edge_to(EdgeId(ans))
        return EdgeId(ans)

    def _next_edge_to(self, edge: EdgeId) -> EdgeId:
        id_addr = self._edge_address(edge)
        ans, _ = self._read_uint_from_edges(EdgeAddress(id_addr + 5 * self.SIZE_UINT))
        return EdgeId(ans)

    def _next_edge_from(self, edge: EdgeId) -> EdgeId:
        id_addr = self._edge_address(edge)
        ans, _ = self._read_uint_from_edges(EdgeAddress(id_addr + 3 * self.SIZE_UINT))
        return EdgeId(ans)

    def _prev_edge_to(self, edge: EdgeId) -> EdgeId:
        id_addr = self._edge_address(edge)
        ans, _ = self._read_uint_from_edges(EdgeAddress(id_addr + 4 * self.SIZE_UINT))
        return EdgeId(ans)

    def _prev_edge_from(self, edge: EdgeId) -> EdgeId:
        id_addr = self._edge_address(edge)
        ans, _ = self._read_uint_from_edges(EdgeAddress(id_addr + 2 * self.SIZE_UINT))
        return EdgeId(ans)

    def _node_id_address(self, nid: NodeId) -> NodeIdAddress:
        return NodeIdAddress(nid * self.SIZE_NODE_ID)

    def _edge_address(self, eid: EdgeId) -> EdgeAddress:
        return EdgeAddress(eid * self.SIZE_EDGE_ID)

    def _write_property_addr_of_node_id(self, nid: NodeId,
                                        addr: PropertyAddress) -> None:
        node_id_address = self._node_id_address(nid)
        s = self._write_uint_to_node_ids(Uint(addr), node_id_address)  # property address
        s = self._write_uint_to_node_ids(Uint(self.EDGE_ID_NONE), s)  # first edge from this node
        self._write_uint_to_node_ids(Uint(self.EDGE_ID_NONE), s)  # first edge to this node

    def create_node(self, node: Node) -> NodeId:
        cur_id = self._read_cur_node_id()
        next_id = NodeId(cur_id + 1)
        cur_addr = self._read_cur_addr()
        next_addr = self._write_node_to_properties(node, cur_addr)
        self._write_cur_addr(next_addr)
        self._write_cur_node_id(next_id)

        self._write_property_addr_of_node_id(cur_id, cur_addr)
        return cur_id

    def _read_uint_from_node_ids(self, f: NodeIdAddress) -> Tuple[Uint, NodeIdAddress]:
        t = NodeIdAddress(f + self.SIZE_UINT)
        m = self._ensure_node_ids_mmap_length(t)
        return self._unpack_uint(m[f:t]), t

    def _read_uint_from_edges(self, f: EdgeAddress) -> Tuple[Uint, EdgeAddress]:
        t = EdgeAddress(f + self.SIZE_UINT)
        m = self._ensure_edges_mmap_length(t)
        return self._unpack_uint(m[f:t]), t

    def _read_float_from_properties(self, cur: PropertyAddress) -> Tuple[Float, PropertyAddress]:
        t = PropertyAddress(cur + self.SIZE_FLOAT)
        m = self._ensure_properties_mmap_length(t)
        return self._unpack_float(m[cur:t]), t

    def _read_uint_from_properties(self, f: PropertyAddress) -> Tuple[Uint, PropertyAddress]:
        t = PropertyAddress(f + self.SIZE_UINT)
        m = self._ensure_properties_mmap_length(t)
        return self._unpack_uint(m[f:t]), t

    def _read_int_from_properties(self, cur: PropertyAddress) -> Tuple[Int, PropertyAddress]:
        t = PropertyAddress(cur + self.SIZE_INT)
        m = self._ensure_properties_mmap_length(t)
        return self._unpack_int(m[cur:t]), t

    def _read_bool_from_properties(self, f: PropertyAddress) -> Tuple[Bool, PropertyAddress]:
        t = PropertyAddress(f + self.SIZE_BOOL)
        m = self._ensure_properties_mmap_length(t)
        return self._unpack_bool(m[f:t]), t

    def _read_text_from_properties(self, f: PropertyAddress) -> Tuple[Text, PropertyAddress]:
        length, bytes_f = self._read_uint_from_properties(f)
        bytes_t = PropertyAddress(bytes_f + length)
        m = self._ensure_properties_mmap_length(bytes_t)
        bts = m[bytes_f:bytes_t]
        return self._unpack_text_bytes(bts), bytes_t

    def _node_address(self, nid: NodeId) -> PropertyAddress:
        id_addr = self._node_id_address(nid)
        ans, _ = self._read_uint_from_node_ids(id_addr)
        return PropertyAddress(ans)

    def _read_first_edge_from_node(self, nid: NodeId) -> EdgeId:
        id_addr = self._node_id_address(nid)
        ans, _ = self._read_uint_from_node_ids(NodeIdAddress(id_addr + self.SIZE_UINT))
        return EdgeId(ans)

    def _read_first_edge_to_node(self, nid: NodeId) -> EdgeId:
        id_addr = self._node_id_address(nid)
        ans, _ = self._read_uint_from_node_ids(NodeIdAddress(id_addr + 2 * self.SIZE_UINT))
        return EdgeId(ans)

    def _read_value_from_properties(self, cur: PropertyAddress) -> Tuple[Any, PropertyAddress]:
        val_desc, cur = self._read_int_from_properties(cur)
        if val_desc >= 0:
            t = PropertyAddress(cur + val_desc)
            m = self._ensure_properties_mmap_length(t)
            text_bytes = m[cur:t]
            return self._unpack_text_bytes(text_bytes), t
        if val_desc == self.TYPE_BOOL:
            return self._read_bool_from_properties(cur)
        if val_desc == self.TYPE_INT:
            return self._read_int_from_properties(cur)
        if val_desc == self.TYPE_UINT:
            return self._read_uint_from_properties(cur)
        if val_desc == self.TYPE_FLOAT:
            return self._read_float_from_properties(cur)
        raise Exception('Unknown val_desc ' + str(val_desc))

    def _read_node_from_properties(self, naddr: PropertyAddress) -> Tuple[Node, PropertyAddress]:
        ans: Node = Node({})
        cur: PropertyAddress = naddr
        rec_len, cur = self._read_uint_from_properties(cur)
        ans['record_address'] = naddr
        ans['record_length'] = rec_len
        ans['props'] = {}
        num_props, cur = self._read_uint_from_properties(cur)
        for _ in repeat(None, num_props):
            k, cur = self._read_text_from_properties(cur)
            v, cur = self._read_value_from_properties(cur)
            ans['props'][k] = v
        return ans, cur

    def _read_edge(self, eaddr: EdgeAddress) -> Tuple[Edge, EdgeAddress, PropertyAddress]:
        ans: Edge = Edge({})
        ans['fnid'], et = self._read_uint_from_edges(eaddr)
        ans['tnid'], et = self._read_uint_from_edges(et)
        ans['prev_1'], et = self._read_uint_from_edges(et)
        ans['next_1'], et = self._read_uint_from_edges(et)
        ans['prev_2'], et = self._read_uint_from_edges(et)
        ans['next_2'], et = self._read_uint_from_edges(et)
        props_addr, et = self._read_uint_from_edges(et)
        # props_addr, et = self._read_uint_from_edges(EdgeAddress(eaddr + 6 * self.SIZE_UINT))
        node, pt = self._read_node_from_properties(PropertyAddress(props_addr))
        ans['props'] = node['props']
        return ans, et, pt

    def get_node(self, nid: NodeId) -> Optional[Node]:
        """
        Get node by ID

        Returns
        -------
        Node by the given ID or None if such node doesn't exist
        """
        node_addr = self._node_address(nid)
        if node_addr == 0:
            return None
        ans, _ = self._read_node_from_properties(node_addr)
        return ans

    def get_edge(self, eid: EdgeId) -> Optional[Edge]:
        """
        Get edge by ID

        Returns
        -------
        Edge by the given ID or None if such edge doesn't exist
        """
        edge_addr = self._edge_address(eid)
        if self._read_uint_from_edges(edge_addr) == self.NODE_ID_NONE:
            return None
        ans, _, _ = self._read_edge(edge_addr)
        return ans

    def get_node_ids(self) -> Iterable[NodeId]:
        nid = NodeId(1)
        cur_nid = self._read_cur_node_id()
        while nid < cur_nid:
            if self._read_uint_from_node_ids(self._node_id_address(nid)) != self.PROPERTY_ADDRESS_NONE:
                yield nid
            nid += 1

    def get_edge_ids(self) -> Iterable[EdgeId]:
        cur_eid = self._read_cur_edge_id()
        eid = EdgeId(1)
        while eid < cur_eid:
            edge, _, _ = self._read_edge(self._edge_address(eid))
            if edge['fnid'] != self.EDGE_ID_NONE:
                yield eid
            eid += 1

    def _write_packed_node_to_properties(self, packed: bytes,
                                         cur: PropertyAddress) -> PropertyAddress:
        t = PropertyAddress(cur + len(packed))
        m = self._ensure_properties_mmap_length(t)
        m[cur:t] = packed
        return t

    def update_node(self, nid: NodeId, n: Node) -> None:
        """
        Replace properties of a node by ID
        """
        old_addr = self._node_address(nid)
        node, _ = self._read_node_from_properties(old_addr)
        if node is None:
            raise Exception('Updating a non-existing node nid=' + str(nid))
        packed = self._pack_node(n)
        new_rec_len = len(packed)
        old_rec_len = node['record_length']
        if new_rec_len == old_rec_len:
            self._write_packed_node_to_properties(packed, old_addr)
        else:
            cur_addr = self._read_cur_addr()
            cur_next_node = self._write_packed_node_to_properties(packed, cur_addr)
            self._write_cur_addr(cur_next_node)

    def delete_node(self, nid: NodeId) -> None:
        """
        Delete node by ID. Edges of this node are left as they are
        """
        self._write_property_addr_of_node_id(nid, self.PROPERTY_ADDRESS_NONE)

    def create_edge(self, from_nid: NodeId,
                    to_nid: NodeId, edge_nid: NodeId) -> EdgeId:
        cur_id = self._read_cur_edge_id()
        next_id = EdgeId(cur_id + 1)
        self._write_edge_to_edges(from_nid, to_nid, edge_nid, cur_id)
        self._write_cur_edge_id(next_id)
        return cur_id

    def remove_edge(self, eid: EdgeId) -> None:
        eaddr = self._edge_address(eid)
        fr, t = self._read_uint_from_edges(eaddr)
        to, t = self._read_uint_from_edges(t)
        if self._read_first_edge_from_node(NodeId(fr)) == eid:
            nxt = self._next_edge_from(eid)
            self._set_first_edge_from(NodeId(fr), nxt)
            self._set_prev_edge_from(self.EDGE_ID_NONE, nxt)
        else:
            nxt = self._next_edge_from(eid)
            prev = self._prev_edge_from(eid)
            self._set_prev_edge_from(prev, nxt)
            self._set_next_edge_from(prev, nxt)
        if self._read_first_edge_to_node(NodeId(to)) == eid:
            nxt = self._next_edge_to(eid)
            self._set_first_edge_to(NodeId(to), nxt)
            self._set_prev_edge_to(self.EDGE_ID_NONE, nxt)
        else:
            nxt = self._next_edge_to(eid)
            prev = self._prev_edge_to(eid)
            self._set_prev_edge_to(prev, nxt)
            self._set_next_edge_to(prev, nxt)
        self._write_uint_to_edges(Uint(self.NODE_ID_NONE), eaddr)

    def edges_from(self, nid: NodeId) -> Iterable[EdgeId]:
        eid = self._read_first_edge_from_node(nid)
        while eid != self.EDGE_ID_NONE:
            yield eid
            eid = self._next_edge_from(eid)

    def edges_to(self, nid: NodeId) -> Iterable[EdgeId]:
        eid = self._read_first_edge_to_node(nid)
        while eid != self.EDGE_ID_NONE:
            yield eid
            eid = self._next_edge_to(eid)

    def close(self):
        self._properties_mmap.close()
        self._node_ids_mmap.close()
        self._edges_mmap.close()

        self._properties_file.close()
        self._node_ids_file.close()
        self._edges_file.close()
