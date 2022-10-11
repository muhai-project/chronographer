# -*- coding: utf-8 -*-
""" Helpers related to streamlit frontend """
import streamlit as st


def on_click_refresh_filters():
    """ Refreshing set of parameters to compare """
    st.session_state.param_1 = False
    st.session_state.param_2 = False
    st.session_state["experiments_run"] = False


def on_click_refresh_common_params():
    """ Refreshing common parameters for search """
    st.session_state.common_params = False


def init_var(var_list):
    """ Initialising list of key, val in session state if not there """
    for key_, val_ in [(key, val) for key, val in var_list if key not in st.session_state]:
        st.session_state[key_] = val_


def write_params(id_set):
    """ Displaying params of set of filters {id_set} """
    def helper_val(input_var):
        return 1 if input_var else 0
    def helper_key(input_var):
        return f"{input_var}_{id_set}"
    st.markdown(f"""
    **domain_range: {helper_val(st.session_state[helper_key('domain_range')])}\t|
    ranking: {st.session_state[helper_key('ranking')]}**  
    **WHO: {helper_val(st.session_state[helper_key('who')])}\t|
    WHAT: {helper_val(st.session_state[helper_key('what')])}\t|
    WHERE: {helper_val(st.session_state[helper_key('when')])}\t|
    WHEN: {helper_val(st.session_state[helper_key('where')])}**
    """)


def write_path_expanded(path):
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


def write_nodes_expanded(nodes):
    """ Listing all nodes in MD format """
    for node in nodes:
        st.markdown(f"* {node}")

def write_metrics(data):
    """ Writing rounded metrics """
    metrics = {key: round(val, 2) for key, val in data.items()}
    st.write(metrics)
