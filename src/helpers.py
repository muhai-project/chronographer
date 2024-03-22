# -*- coding: utf-8 -*-
""" 
Helpers
"""
import pandas as pd
from pyvis.network import Network

def rdflib_to_pyvis_html(graph, save_path):
    net = Network(height="100%", width="100%", directed=True)
    
    nodes = set(s for s, _, _ in graph).union(set(o for _, _, o in graph))
    for node in nodes:
        net.add_node(node, label=node)
    for s, p, o in graph:
        net.add_edge(s, o, label=p)
    
    # Generate HTML code for the network visualization
    net.show(save_path)

def rdflib_to_pd(graph):
    """ Rdflib graph to pandas df with columns ["subject", "predicate", "object"] """
    df = pd.DataFrame(columns=['subject', 'predicate', 'object'])
    for subj, pred, obj in graph:
        df.loc[df.shape[0]] = [str(subj), str(pred), str(obj)]
    return df