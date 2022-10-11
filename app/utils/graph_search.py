# -*- coding: utf-8 -*-
""" Helpers related to graph search framework """
import os
import pickle
from datetime import datetime
import streamlit as st
import pandas as pd

from src.framework import GraphSearchFramework
from .graph_vis import build_complete_network


def check_variables_for_search():
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
        except Exception:
            st.error(logs[key])
            check_ok = False

    return check_ok


def get_common_base_config():
    """ Update config for search with common params for two sets of filters """
    var_dataset = st.session_state.variables_dataset[st.session_state.dataset]
    event_id = st.session_state.start_node.replace(var_dataset["start_uri"], "")
    return {
        "start": st.session_state.start_node,
        "start_date": st.session_state.start_date,
        "end_date": st.session_state.end_date,
        "iterations": st.session_state.iterations,
        "gold_standard": os.path.join(
            var_dataset["data_files_path"], "gs_events", f"{event_id}.csv"),
        "referents": os.path.join(var_dataset["data_files_path"], "referents", f"{event_id}.json"),
        "name_exp": event_id,
        "dataset_path": var_dataset["dataset_path"],
        "nested_dataset": var_dataset["nested_dataset"],
    }


def get_specific_config(id_set):
    """ Retrieving narrative filter config for set of params {id_set} (1 or 2)"""
    def helper(input_var):
        return 1 if input_var else 0
    ranking_to_val = {"predicate": "pred_object_freq", "entropy": "entropy_pred_object_freq"}
    return {
        "type_ranking": ranking_to_val[st.session_state[f"ranking_{id_set}"]],
        "ordering": {
            "domain_range": helper(st.session_state[f"domain_range_{id_set}"])
        },
        "filtering": {
            "what": helper(st.session_state[f"who_{id_set}"]),
            "where": helper(st.session_state[f"where_{id_set}"]),
            "when": helper(st.session_state[f"when_{id_set}"]),
            "who": helper(st.session_state[f"who_{id_set}"]),
        },
    }


def get_graph_search_info(id_set, base_config):
    """ Returns config for search and folder to save data """
    spec_config = get_specific_config(id_set)
    spec_config.update(base_config)

    dataset = st.session_state["dataset"]
    var_dataset = st.session_state.variables_dataset[dataset]
    event_id = st.session_state.start_node.replace(var_dataset["start_uri"], "")

    def helper(k, val):
        return k if val else ""

    folder_name = \
        "./data/" + dataset.lower() + "_" + event_id + "_" + \
            spec_config['type_ranking'].replace('_freq', '') + \
                "_" + helper('domain_range', spec_config['ordering']['domain_range']) + \
                    "_" + helper('what', spec_config['filtering']['what']) + \
                        "_" + helper('when', spec_config['filtering']['when']) + \
                            "_" + helper('where', spec_config['filtering']['where']) + \
                                "_" + helper('who', spec_config['filtering']['who'])
    return spec_config, folder_name


def run_search_save_info(config, save_folder):
    """ Running search and saving info (graph+stats) to later display results """
    config["rdf_type"] = list(config["rdf_type"].items())
    framework = GraphSearchFramework(config=config)
    framework()
    to_pickle = {
        "config": framework.config,
        "path_expanded": framework.expanded,
        "metrics": framework.metrics_data,
        "nodes_expanded_per_iter": framework.nodes_expanded_per_iter
    }

    with open(f"{save_folder}/framework.pkl", "wb") as openfile:
        pickle.dump(to_pickle, openfile)

    curr_subgraph = framework.subgraph
    curr_nodes_expanded = framework.nodes_expanded_per_iter
    curr_path_expanded = framework.expanded

    for iteration in range(framework.iterations):
        build_complete_network(
            subgraph=curr_subgraph[curr_subgraph.iteration <= iteration+1],
            nodes_expanded=curr_nodes_expanded[curr_nodes_expanded.iteration <= iteration+1],
            path_expanded=curr_path_expanded[curr_path_expanded.iteration <= iteration+1],
            save_file=f"./{save_folder}/subgraph-{iteration+1}.html",
            ground_truth=set(pd.read_csv(config["gold_standard"]).linkDBpediaEn.unique()))
