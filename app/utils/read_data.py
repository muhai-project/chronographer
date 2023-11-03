# -*- coding: utf-8 -*-
""" Mainly opening files + some pre-processing """
import pickle

def read_pickled_data_graph_search(folder: str) -> dict:
    """ Modify index of certain df for better display """
    data = pickle.load(open(f"{folder}/framework.pkl", 'rb'))
    for key, col in [("path_expanded", "iteration"),
                     ("nodes_expanded_per_iter", "iteration")]:
        data[key].set_index(col, inplace=True)
    return data

def get_source_code(html_path: str) -> str:
    """ Return graph visualisation HTML """
    with open(html_path, 'r', encoding='utf-8') as html_file:
        source_code = html_file.read()
    return source_code
