from graphstorage import GraphStorage, Node

s = GraphStorage(True, 'test')

node = Node({
    'is_edge': False,
    'props': {
        'two_uint': 2,
        'рюсски': -1,
        'float_pi': 1.25,
        'bool_true': True,
        'bool_false': False,
        'char_z': 'z',
        'text_hello': 'hello'
    }
})

nid = s.create_node(node)

node2 = s.get_node(nid)

print(node['props'])
print(node2['props'])
assert node['props'] == node2['props']

s.close()
