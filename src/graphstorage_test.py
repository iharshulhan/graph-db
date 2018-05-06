import unittest

from src.graphstorage import GraphStorage, Node, NodeId


class GraphStorageTest(unittest.TestCase):
    def setUp(self):
        self.storage = GraphStorage(True, 'test')

    def tearDown(self):
        self.storage.close()

    def test_node_creation_and_getting(self):
        node = Node({
            'props': {
                'an_int': 2,
                'unicode': 'салəм',
                'float': 1.25,  # 1.25 represents well in binary
                'bool_true': True,
                'bool_false': False,
                'char_z': 'z',
                'text_hello': 'hello'
            }
        })

        nid = self.storage.create_node(node)
        node2 = self.storage.get_node(nid)
        self.assertEqual(node['props'], node2['props'])
        self.assertTrue(nid in self.storage.get_node_ids())

    def test_edge_creation_and_getting(self):
        node = Node({
            'props': {
                'a': 1,
                'b': 2,
                'c': 3
            }
        })
        other_node = Node({
            'props': {
                'useless': True
            }
        })
        edge_props = Node({
            'props': {
                'an_int': 2,
                'unicode': 'салəм',
                'float': 1.25,  # 1.25 represents well in binary
                'bool_true': True,
                'bool_false': False,
                'char_z': 'z',
                'text_hello': 'hello'
            }
        })

        nid = self.storage.create_node(node)
        other_nid = self.storage.create_node(other_node)
        edge_nid = self.storage.create_node(edge_props)

        edge_id = self.storage.create_edge(nid, other_nid, edge_nid)
        edge = self.storage.get_edge(edge_id)
        self.assertEqual(nid, edge['fnid'])
        self.assertEqual(other_nid, edge['tnid'])
        self.assertEqual(edge_props['props'], edge['props'])
        self.assertTrue(edge_id in self.storage.get_edge_ids())

    def test_deleting_edge(self):
        node = Node({
            'props': {
                'a': 1,
                'b': 2,
                'c': 3
            }
        })
        other_node = Node({
            'props': {
                'useless': True
            }
        })
        edge_props_1 = Node({
            'props': {
                'text_hello': 'hello'
            }
        })
        edge_props_2 = Node({
            'props': {
                'an_int': 2
            }
        })
        edge_props_3 = Node({
            'props': {
                'unicode': 'салəм'
            }
        })
        nid = self.storage.create_node(node)
        other_nid = self.storage.create_node(other_node)
        edge_nid = self.storage.create_node(edge_props_1)
        edge_1 = self.storage.create_edge(nid, other_nid, edge_nid)
        edge_nid = self.storage.create_node(edge_props_2)
        edge_2 = self.storage.create_edge(nid, other_nid, edge_nid)
        edge_nid = self.storage.create_node(edge_props_3)
        edge_3 = self.storage.create_edge(nid, other_nid, edge_nid)
        self.assertTrue(edge_2 in self.storage.get_edge_ids())
        self.assertEqual(len(self.storage.edges_from(nid)), 3)
        self.storage.remove_edge(edge_2)
        self.assertEqual(len(self.storage.edges_from(nid)), 2)
        self.assertTrue(edge_2 not in self.storage.get_edge_ids())

    def test_node_none_on_non_existent_nid(self):
        self.assertIsNone(self.storage.get_node(NodeId(123)),
                          'Should be None if no node with such id')

    def test_node_updating_with_same_length(self):
        node = Node({
            'props': {
                'a': 1,
                'b': 2,
                'c': 3
            }
        })
        other_node = Node({
            'props': {
                'useless': True
            }
        })
        nid = self.storage.create_node(node)
        other_nid = self.storage.create_node(other_node)

        node['b'] = 123  # any other int
        self.storage.update_node(nid, node)
        updated = self.storage.get_node(nid)
        other_fresh = self.storage.get_node(other_nid)
        self.assertEqual(node['props'], updated['props'])
        self.assertEqual(other_node['props'], other_fresh['props'])

    def test_node_updating_with_different_length(self):
        node = Node({
            'props': {
                'a': 1,
                'b': 2,
                'c': 3
            }
        })
        other_node = Node({
            'props': {
                'useless': True
            }
        })
        nid = self.storage.create_node(node)
        other_nid = self.storage.create_node(other_node)

        node['b'] = 'text is longer than int'  # other length
        self.storage.update_node(nid, node)
        updated = self.storage.get_node(nid)
        other_fresh = self.storage.get_node(other_nid)
        self.assertEqual(node['props'], updated['props'])
        self.assertEqual(other_node['props'], other_fresh['props'])

    def test_node_deletion(self):
        node = Node({
            'props': {}
        })
        nid = self.storage.create_node(node)
        from_storage = self.storage.get_node(nid)
        self.assertEqual(from_storage['props'], node['props'])
        self.storage.delete_node(nid)
        from_storage_after = self.storage.get_node(nid)
        self.assertIsNone(from_storage_after)


if __name__ == '__main__':
    unittest.main()

