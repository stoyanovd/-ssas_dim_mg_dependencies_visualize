import json
import os
# import xml.etree.ElementTree as ET
from lxml import etree, objectify
import pandas as pd
import re

from secure import local_settings

import numpy as np
from sqlalchemy import create_engine
from sqlalchemy import types as sql_types
import networkx as nx
import matplotlib.pyplot as plt
import graphviz


def clean_xmla_from_namespaces(root):
    # remove namespaces ----
    for elem in root.getiterator():
        if not hasattr(elem.tag, 'find'): continue
        i = elem.tag.find('}')
        if i >= 0:
            elem.tag = elem.tag[i + 1:]

    objectify.deannotate(root, cleanup_namespaces=True)
    # ----


#
# def dump_clean_ssas(xmla_path):
#     tree = etree.parse(xmla_path)
#     root = tree.getroot()
#     clean_xmla_from_namespaces(root)
#     t = etree.tostring(root, pretty_print=True)
#     with open('./temp/clean_ssas.xmla', 'wb') as f:
#         f.write(t)


def parse_ssas_get_links(xmla_path):
    DEBUG_HERE = 0
    tree = etree.parse(xmla_path)
    root = tree.getroot()
    clean_xmla_from_namespaces(root)

    # leaf_name_tag = 'DimensionID' if is_dim_step else 'MeasureGroupID'

    # root tag <Batch>
    tag_with_list = root.find(
        "./ObjectDefinition/Database/Cubes/Cube[Name='" + local_settings.CUBE_NAME + "']/MeasureGroups")

    if DEBUG_HERE:
        print(tag_with_list)
        print('#####')

    # id_list = []
    # name_list = []
    links = []
    links_m2m = []
    for mg in tag_with_list:  # .find('./Dimension'): # .find('./MeasureGroup'):
        # tag Process
        # item_id = dim.findtext('./DimensionID')
        # item_name = dim.findtext('./Name')
        mg_id = mg.findtext('./ID')
        mg_name = mg.findtext('./Name')

        if DEBUG_HERE:
            print(mg_name)
        if mg_name is None:
            continue
        # id_list.append(mg_id)
        # name_list.append(mg_name)

        for dim in mg.find('Dimensions'):
            dim_id = dim.findtext('./CubeDimensionID')
            # do not visualize m2m currently
            if dim.findtext('./MeasureGroupID'):
                links_m2m += [[mg_id, dim_id, dim.findtext('./MeasureGroupID')]]
            else:
                links += [[mg_id, dim_id]]
            if DEBUG_HERE:
                print('is m2m: ', dim.findtext('./MeasureGroupID'), ' ', mg_id, ' -> ', dim_id)
        # <CubeDimensionID>DIM Clients Hrono H1</CubeDimensionID>
        # <Attribute> -> <Type>Granularity</Type>
    return links, links_m2m


interesting_mgs = local_settings.interesting_mgs

dims_to_filter_out = local_settings.dims_to_filter_out


def prepare_links(links):
    # links = np.array(links)
    links = list(filter(lambda l: l[0] in interesting_mgs, links))
    links = list(filter(lambda l: l[1] not in dims_to_filter_out, links))
    links = np.array(links)
    return links


def draw_graph(links):
    # Build a dataframe with your connections
    df = pd.DataFrame({'from': links[:, 0], 'to': links[:, 1]})
    plt.figure(figsize=(17, 17))
    plt.margins(x=0.1, y=0.1)
    # Build your graph
    G = nx.from_pandas_edgelist(df, 'from', 'to')

    # Graph with Custom nodes:
    nx.draw(G, with_labels=True, node_size=100, node_color="skyblue", node_shape="s", alpha=0.5, linewidths=40)
    # plt.show()
    plt.savefig('tmp/graph_1.png')


def way2(links):
    dot = graphviz.Digraph(comment='The Round Table', graph_attr={'rankdir': 'LR'})

    for mg in links[:, 0]:
        dot.node(mg, mg)
    for dim in links[:, 1]:
        dot.node(dim, dim)

    for link in links:
        # dot.edge(link[0], link[1], constraint='false')
        dot.edge(link[0], link[1])

    with open('tmp/graph_desc_hrono_1.txt', 'w') as f:
        f.write(dot.source)

    dot.render('tmp/graph_hrono_1.pdf', format='svg')


def dump_for_kumu(links_direct, links_m2m):
    elements = [
        {
            'label': mg,
            'type': 'MG'
        }
        for mg in np.unique(np.concatenate((links_direct[:, 0], links_m2m[:, 0])))
    ]
    elements += [
        {
            'label': pt,
            'type': 'DIM'
        }
        for pt in np.unique(np.concatenate((links_direct[:, 1], links_m2m[:, 1])))
    ]
    connections = [
        {
            "from": link[0],
            "to": link[1],
            "type": "Direct",
            "direction": "directed",
        } for link in links_direct
    ]
    connections += [
        {
            "from": link[0],
            "to": link[1],
            "type": "M2M",
            "direction": "directed",
            'label': link[2],
        } for link in links_m2m
    ]
    print(elements)
    print(connections)
    res = {'elements': elements, 'connections': connections}

    json_path = 'tmp/graphhrono_for_kumu.json'
    json.dump(res, open(json_path, 'w'))


def main():
    full_ssas_path = local_settings.FULL_SSAS_XMLA_DUMP_PATH

    links_direct, links_m2m = parse_ssas_get_links(full_ssas_path)

    links_direct = prepare_links(links_direct)
    links_m2m = prepare_links(links_m2m)

    # draw_graph(links_cube)
    # way2(links_cube)

    dump_for_kumu(links_direct, links_m2m)
    return

    df = pd.DataFrame(to_dict)
    df = df.sort_values(['obj_type', 'names'])
    print(df)
    result_path = './temp/dim_mg_in_jobs.csv'
    df.to_csv(result_path)


if __name__ == '__main__':
    main()
