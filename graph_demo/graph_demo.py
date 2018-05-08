"""
Social circles: Facebook graph demo dataset

https://snap.stanford.edu/data/egonets-Facebook.html
"""
import re
from distribute_graph_dbms.dbms import DBMS
from distribute_graph_dbms.start_engines import start_engines
from time import sleep
import logging


def load_social_circles_facebook(dbms: DBMS) -> None:
    ego_ids = ['0', '107', '348', '414', '686', '698', '1684', '1912', '3437', '3980']
    prefix = 'dataset/'
    circles_suffix = '.circles'
    edges_suffix = '.edges'
    egofeat_suffix = '.egofeat'
    feat_suffix = '.feat'
    featnames_suffix = '.featnames'

    node_props = {}
    edges = {}
    circles = {}
    for ego_id in ego_ids[:1]:
        print('Parsing ego id', ego_id)

        circles_filename = prefix + ego_id + circles_suffix
        edges_filename = prefix + ego_id + edges_suffix
        egofeat_filename = prefix + ego_id + egofeat_suffix
        feat_filename = prefix + ego_id + feat_suffix
        featnames_filename = prefix + ego_id + featnames_suffix

        with open(circles_filename, 'r') as circles_file:
            for line in circles_file:
                words = line.split()
                circle_name = words[0] + '_' + ego_id
                circles[circle_name] = [ego_id] + words[1:]

        featnames = []
        with open(featnames_filename, 'r') as featnames_file:
            for line in featnames_file:
                _, featname, featval = re.search(r'^(\d+) (.*);(.*)$', line).groups()
                featnames.append({'key': featname, 'val': featval})

        if ego_id not in node_props:
            node_props[ego_id] = {}
        with open(egofeat_filename, 'r') as egofeat_file:
            for i, val in enumerate(egofeat_file.readline().split()):
                if val != '1':
                    continue
                feat = featnames[i]
                node_props[ego_id][feat['key']] = feat['val']

        with open(feat_filename, 'r') as feat_file:
            for line in feat_file:
                words = line.split()
                node_id = words[0]
                for i, val in enumerate(words[1:]):
                    if val != '1':
                        continue
                    feat = featnames[i]
                    if node_id not in node_props:
                        node_props[node_id] = {}
                    node_props[node_id][feat['key']] = feat['val']
        if ego_id not in edges:
            edges[ego_id] = {}
        with open(edges_filename) as edges_file:
            for line in edges_file:
                u, v = line.split()
                if u not in edges:
                    edges[u] = {}
                if v not in edges:
                    edges[v] = {}
                edges[u][v] = True
                edges[v][u] = True
                edges[ego_id][u] = True
                edges[u][ego_id] = True
                edges[ego_id][v] = True
                edges[v][ego_id] = True

    # Put data to graph
    node_id_mapping = {}
    print(len(circles), 'nodes')
    # make circle nodes
    for circle_name in circles:
        props = {'label': 'circle', 'name': circle_name}
        node_id_mapping[circle_name] = dbms.add_node(props)
    # make user nodes
    print(len(node_props), 'nodes')
    for fb_id in node_props:
        props = node_props[fb_id]
        props['label'] = 'user'
        props['fb_id'] = fb_id
        node_id_mapping[fb_id] = dbms.add_node(props)
    print('EGO NODE IS', node_id_mapping[ego_id])
    # make circle edges
    tot_edges = 0
    for circle_name in circles:
        ids = circles[circle_name]
        cnid = node_id_mapping[circle_name]
        for nid in ids:
            tot_edges += 1
            unid = node_id_mapping[nid]
            print('CIRCLE EDGE', unid, cnid)
            dbms.add_edge(unid, cnid, {})
    print(tot_edges, 'already done edges')
    # make regular edges
    for u in edges:
        unid = node_id_mapping[u]
        for v in edges[u]:
            tot_edges += 1
            if tot_edges % 1000 == 0:
                print(tot_edges, 'edges already added')
            vnid = node_id_mapping[v]
            dbms.add_edge(unid, vnid, {})
    print('Loaded!')


if __name__ == '__main__':
    log = logging.getLogger('werkzeug')
    log.disabled = True
    engines = start_engines()
    sleep(2)
    dbms = DBMS(engines)
    load_social_circles_facebook(dbms)

    zero_node = dbms.find_nodes({'equal_props': {'fb_id': '0'}})[0]
    print('zero_node', zero_node)
    print(zero_node['node_id'], {'equal_props': {'label': 'user'}})
    three_friends = dbms.find_neighbours(zero_node['node_id'], 3, {'equal_props': {'label': 'user'}})
    for friend in list(three_friends)[:10]:
        print('My three-friend', friend['props']['fb_id'])
    other_gender = dbms.find_nodes({
        'not_equal_props': {'gender': zero_node['props']['gender']},
        'equal_props': {'label': 'user'}
    })
    print()
    print('My gender is', zero_node['props']['gender'])
    for other in other_gender[:10]:
        print('But', other['props']['fb_id'], 'has other gender', other['props']['gender'])
    print('End :-)')




