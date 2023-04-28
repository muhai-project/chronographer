# -*- coding: utf-8 -*-
""" Helpers related to streamlit frontend """
import os
import pandas as pd
import streamlit as st
from .variables import DEFAULT_VARIABLES
from .content import RES_ITERATION


def on_click_refresh_system_params():
    """ Refreshing set of parameters to compare """

    st.session_state["experiments_run"] = False

    for key in ["submit_all_vs_subset", "param"]:
        for id_set in ["1", "2"]:
            st.session_state[f"{key}_{id_set}"] = False

    for key in ["filters_", "expand_all_vs_subset_", "nb_random_", "walk_",
                "ranking_", "domain_range_"]:
        for id_set in ["1", "2"]:
            st.session_state[key+id_set] = DEFAULT_VARIABLES[key]


def on_click_set_true(key):
    """ Setting a param in session_statet to True """
    st.session_state[key] = True


def on_click_refresh_common_params():
    """ Refreshing common parameters for search """
    st.session_state.submit_max_uri = False
    st.session_state.submit_common_params = False
    st.session_state.common_params = False

    for key in ["dataset", "start_node", "start_date", "end_date",
                "iterations", "max_uri", "max_uri_val"]:
        st.session_state[key] = DEFAULT_VARIABLES[key]


def init_var(var_list: list[(str, object)]):
    """ Initialising list of key, val in session state if not there """
    for key_, val_ in [(key, val) for key, val in var_list if key not in st.session_state]:
        st.session_state[key_] = val_


def write_params(id_set: str):
    """ Displaying params of set of filters {id_set} """

    filters = st.session_state[f"filters_{id_set}"]
    st.markdown(f"**Filters to prune the search space:** {filters}")
    expand_all_vs_subset = st.session_state[f"expand_all_vs_subset_{id_set}"]
    st.markdown(f"**Expansion of nodes at each iteration:** {expand_all_vs_subset}")

    if expand_all_vs_subset == "subset-random":
        nb_random = st.session_state[f"nb_random_{id_set}"]
        st.markdown(f"**Number of random nodes:** {nb_random}")

    if expand_all_vs_subset == "subset-informed":
        domain_range = 1 if st.session_state[f'domain_range_{id_set}'] else 0
        ranking = st.session_state[f"ranking_{id_set}"]
        st.markdown(f"""
        **domain_range: {domain_range}\t|
        ranking: {ranking}**
        """)


def write_path_expanded(path: str):
    """ Detailing meaning of path expanded """
    if 'ingoing' in path:
        [predicate_t, object_t] = path.split("ingoing-")[1].split(';')
        st.markdown(f"""
        **Type:** ingoing  
        **predicate:** {predicate_t}   
        **object:** {object_t}""")
    else:
        [subject_t, predicate_t] = path.split("outgoing-")[1].split(';')
        st.markdown(f"""
        **Type:** outgoing    
        **subject:** {subject_t}   
        **predicate:** {predicate_t}""")


def write_nodes_expanded(nodes: list[str]):
    """ Listing all nodes in MD format """
    for node in nodes:
        st.markdown(f"* {node}")


def write_metrics(data: dict):
    """ Writing rounded metrics """
    metrics = {key: round(val, 2) for key, val in data.items()}
    st.write(metrics)


def get_max_iteration_nb(id_set: int) -> int:
    """ For one search, get the last iteration number """
    files = [x for x in os.listdir(st.session_state[f"folder_{id_set}"]) \
        if x.endswith(".html")]
    return max(int(x.replace(".html", "").split("-")[-1]) for x in files)


def write_path_node_info(iteration: int, data: pd.core.frame.DataFrame, id_set: str):
    """ Additional info on paths and nodes at each iteration """
    if st.session_state[f"walk_{id_set}"] == "informed":
        # Displaying paths expanded at each iteration and nodes
        with st.expander(RES_ITERATION["path_chosen"].format("at", iteration)):
            if 1 < iteration < st.session_state["iterations"]:
                path = data["path_expanded"][data["path_expanded"] \
                .index == iteration-1].path_expanded.values[0]
                write_path_expanded(path)
            else:
                st.write("N/A since first or last iteration")

        with st.expander(RES_ITERATION["node_expanded"].format(iteration)):
            nodes_expanded = data["nodes_expanded_per_iter"] \
                [data["nodes_expanded_per_iter"].index == iteration] \
                    .node_expanded.values[0]
            write_nodes_expanded(nodes=nodes_expanded)

        with st.expander(RES_ITERATION["path_chosen"].format("for", iteration+1)):
            path = data["path_expanded"][data["path_expanded"] \
                .index == iteration].path_expanded.values[0]
            write_path_expanded(path)

    if st.session_state[f"walk_{id_set}"] == "random":
        # No path expanded, only node information
        with st.expander(RES_ITERATION["node_expanded"].format(iteration)):
            if iteration == 1:
                node_expanded = data["nodes_expanded_per_iter"] \
                [data["nodes_expanded_per_iter"].index == iteration] \
                    .node_expanded.values[0][0]
                st.markdown(f"* {node_expanded}")
            else:
                nodes_expanded = data["path_expanded"] \
                [data["path_expanded"].index == iteration]
                for _, row in nodes_expanded.iterrows():
                    st.markdown(f"* {row.node_expanded}")
                    write_path_expanded(row.path_expanded)
