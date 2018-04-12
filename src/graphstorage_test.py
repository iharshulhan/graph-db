import unittest

from graphstorage import GraphStorage, Node, NodeId


class GraphStorageTest(unittest.TestCase):
    def setUp(self):
        self.storage = GraphStorage(True, 'test')

    def tearDown(self):
        self.storage.close()

    def test_node_creation_and_getting(self):
        node = Node({
            'is_edge': False,
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
        self.assertEqual(node['is_edge'], node2['is_edge'])

    def test_node_none_on_non_existent_nid(self):
        self.assertIsNone(self.storage.get_node(NodeId(123)),
                          'Should be None if no node with such id')

    def test_node_updating_with_same_length(self):
        node = Node({
            'is_edge': False,
            'props': {
                'a': 1,
                'b': 2,
                'c': 3
            }
        })
        other_node = Node({
            'is_edge': True,
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
            'is_edge': False,
            'props': {
                'a': 1,
                'b': 2,
                'c': 3
            }
        })
        other_node = Node({
            'is_edge': True,
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
            'is_edge': False,
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

