# -*- coding: utf-8 -*-
""" Helpers related to graph search framework """
import os
import json
import pickle
from datetime import datetime
import streamlit as st
import pandas as pd

from src.framework import GraphSearchFramework
from .graph_vis import build_complete_network
from .streamlit_helpers import init_var


def check_variables_for_search() -> bool:
    """ Checking that variables are suited for search """
    dataset = st.session_state.dataset
    var_dataset = st.session_state.variables_dataset[dataset]
    logs = st.session_state.logs_variables_search
    check_ok = True

    if not st.session_state.start_node.startswith(var_dataset["start_uri"]):
        st.error(logs["start_node_empty"].format(var_dataset["start_uri"], dataset))
        check_ok = False
    else:
        event_id = st.session_state.start_node.replace(var_dataset["start_uri"], "")
        files_to_check = os.listdir(os.path.join(var_dataset["data_files_path"], "gs_events"))
        if f"{event_id}.csv" not in files_to_check:
            st.error(logs["start_node_no_gs"].format(var_dataset["data_files_path"], event_id))
            check_ok = False
        files_to_check = os.listdir(os.path.join(var_dataset["data_files_path"], "referents"))
        if f"{event_id}.json" not in files_to_check:
            st.error(logs["start_node_no_ref"].format(var_dataset["data_files_path"], event_id))
            check_ok = False

    for var, key in [(st.session_state.start_date, 'start_date'),
                     (st.session_state.end_date, 'end_date')]:
        try:
            datetime.strptime(var, "%Y-%m-%d")
        except ValueError:
            st.error(logs[key])
            check_ok = False

    return check_ok


def get_common_base_config() -> dict:
    """ Update config for search with common params for two sets of filters """
    var_dataset = st.session_state.variables_dataset[st.session_state.dataset]
    event_id = st.session_state.start_node.replace(var_dataset["start_uri"], "")
    # Common params for all systems
    config = {
        "start": st.session_state.start_node,
        "start_date": st.session_state.start_date,
        "end_date": st.session_state.end_date,
        "gold_standard": os.path.join(
            var_dataset["data_files_path"], "gs_events", f"{event_id}.csv"),
        "referents": os.path.join(var_dataset["data_files_path"], "referents", f"{event_id}.json"),
        "name_exp": event_id,
        "iterations": st.session_state.iterations,
        "dataset_path": var_dataset["dataset_path"],
        "nested_dataset": var_dataset["nested_dataset"],
    }
    # Only for systems whose node expansion is random
    if isinstance(st.session_state.max_uri_val, int):
        config.update({"max_uri": st.session_state.max_uri_val})
    return config


def get_specific_config(id_set: str) -> dict:
    """ Retrieving narrative filter config for set of params {id_set} ('1' or '2')"""
    uri_limit = st.session_state[f"nb_random_{id_set}"] if \
            st.session_state[f"nb_random_{id_set}"] else 'all'
    init_var([('uri_limit', uri_limit)])
    def helper(target_var, all_vars):
        return 1 if target_var in all_vars else 0
    return {
        "type_ranking": st.session_state[f"ranking_{id_set}"],
        "ordering": {
            "domain_range": 1 if st.session_state[f"domain_range_{id_set}"] else 0
        },
        "filtering": {
            "what": helper("what", st.session_state[f"filters_{id_set}"]),
            "where": helper("where", st.session_state[f"filters_{id_set}"]),
            "when": helper("when", st.session_state[f"filters_{id_set}"]),
            "who": helper("who", st.session_state[f"filters_{id_set}"]),
        },
        "uri_limit": st.session_state[f"nb_random_{id_set}"] if \
            st.session_state[f"nb_random_{id_set}"] and \
                st.session_state[f"expand_all_vs_subset_{id_set}"] == "subset-random" else 'all'
    }


def get_graph_search_info(id_set: str, base_config: dict) -> (dict, str):
    """ Returns config for search and folder to save data """
    spec_config = get_specific_config(id_set)
    spec_config.update(base_config)

    dataset = st.session_state["dataset"]
    var_dataset = st.session_state.variables_dataset[dataset]
    event_id = st.session_state.start_node.replace(var_dataset["start_uri"], "")

    def helper(k, val):
        return k if val else ""

    max_uri = spec_config["max_uri"] if "max_uri" in spec_config \
        else float('inf')

    folder_name = \
        "./data/" + dataset.lower() + "_" + event_id + "_" + \
            st.session_state[f"walk_{id_set}"] + "_iterations_" + \
                str(st.session_state["iterations"]) + "_max_uri_" + \
                str(max_uri) + "_uri_limit_" + \
                    str(spec_config["uri_limit"]) + "_" + \
                        spec_config['type_ranking'].replace('_freq', '') + \
                            "_" + helper(
                                'domain_range', spec_config['ordering']['domain_range']) + \
                                "_" + helper('what', spec_config['filtering']['what']) + \
                                    "_" + helper('when', spec_config['filtering']['when']) + \
                                        "_" + helper('where', spec_config['filtering']['where']) + \
                                            "_" + helper('who', spec_config['filtering']['who'])
    return spec_config, folder_name


def run_search_save_info(config: dict, save_folder: str, walk: str):
    """ Running search and saving info (graph+stats) to later display results """
    config["rdf_type"] = list(config["rdf_type"].items())
    framework = GraphSearchFramework(
        config=config, walk=walk,
        keep_only_last=False)
    framework()
    to_pickle = {
        "config": framework.config,
        "path_expanded": framework.expanded,
        "metrics": framework.metrics_data,
        "nodes_expanded_per_iter": framework.nodes_expanded_per_iter
    }

    with open(f"{save_folder}/framework.pkl", "wb") as openfile:
        pickle.dump(to_pickle, openfile)

    with open(f"{save_folder}/config.json", "w+", encoding='utf-8') as openfile:
        json.dump(framework.config, openfile)

    curr_subgraph = framework.subgraph
    curr_nodes_expanded = framework.nodes_expanded_per_iter
    curr_path_expanded = framework.expanded

    # Building html files for visualisation
    for iteration in range(framework.last_iteration):
        build_complete_network(
            subgraph=curr_subgraph[curr_subgraph.iteration <= iteration+1],
            nodes_expanded=curr_nodes_expanded[curr_nodes_expanded.iteration <= iteration+1],
            path_expanded=curr_path_expanded[curr_path_expanded.iteration <= iteration+1],
            save_file=f"./{save_folder}/subgraph-{iteration+1}.html",
            ground_truth=set(pd.read_csv(config["gold_standard"]).linkDBpediaEn.unique()))
