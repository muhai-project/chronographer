# -*- coding: utf-8 -*-
""" Main streamlit app """
import os
from datetime import datetime
import streamlit as st
import streamlit.components.v1 as components

from utils.graph_search import check_variables_for_search, \
    get_common_base_config, get_graph_search_info, \
        run_search_save_info
from utils.streamlit_helpers import on_click_refresh_filters, init_var, \
    on_click_refresh_common_params, write_params, write_nodes_expanded, write_path_expanded, \
        write_metrics
from utils.read_data import read_pickled_data_graph_search, get_source_code

from variables import VARIABLES_DATASET
from content import LOGS_VARIABLES_SEARCH, BASE_CONFIG, EVENT_INPUT, \
    FILTERS_INPUT, GRAPH_SEARCH, RES_COMPARISON, RES_ITERATION


# Setting layout + init session state varialbes
st.set_page_config(layout="wide")
init_var(var_list=[("variables_dataset", VARIABLES_DATASET),
                   ("logs_variables_search", LOGS_VARIABLES_SEARCH),
                   ("base_config", BASE_CONFIG),
                   ("experiments_run", False)])

st.title("Comparing the impact of narrative filters")

# Container for event selection
with st.container():
    st.markdown(EVENT_INPUT['headline'])

    with st.expander(EVENT_INPUT['expand']):

        with st.form("common_params_form"):
            dataset = st.selectbox(
                EVENT_INPUT['select_dataset'],
                tuple(sorted(tuple(VARIABLES_DATASET.keys()))), key="dataset")

            start_node = st.text_input(
                EVENT_INPUT['select_start_node'],
                value="http://dbpedia.org/resource/French_Revolution",
                key="start_node")

            col1_start_date, col2_end_date = st.columns(2)

            with col1_start_date:
                start_date = st.text_input(EVENT_INPUT['select_start_date'],
                                           value="1789-05-05", key="start_date")

            with col2_end_date:
                end_date = st.text_input(EVENT_INPUT['select_end_date'],
                                         value="1799-12-31", key="end_date")

            iterations = st.number_input(label=EVENT_INPUT['iterations'], min_value=1,
                                        max_value=30, value=5, step=1, key="iterations")
            submit_common_params = st.form_submit_button(EVENT_INPUT['submit'])

        init_var([('common_params', False)])
        if submit_common_params:
            st.session_state.common_params = True
        refresh_common_params = st.button(EVENT_INPUT['refresh'],
                                          on_click=on_click_refresh_common_params)

    if not st.session_state.common_params:
        st.warning(EVENT_INPUT['no_submit_warning'])
    else:
        st.empty()


# Container for filters selection
with st.container():
    st.markdown(FILTERS_INPUT['headline'])

    with st.expander(FILTERS_INPUT['expand']):
        col1_filters_1, col2_filters_2 = st.columns(2)

        with col1_filters_1:
            st.markdown(FILTERS_INPUT['set_filters_1'])
            with st.form("filters_1"):
                ranking_1 = st.selectbox(label=FILTERS_INPUT["ranking"],
                                         options=["predicate", "entropy"],
                                         key="ranking_1")
                domain_range_1 = st.checkbox(FILTERS_INPUT["domain_range"],
                                             key="domain_range_1", value=True)
                who_1 = st.checkbox(FILTERS_INPUT["who_filter"], key="who_1", value=True)
                what_1 = st.checkbox(FILTERS_INPUT["what_filter"], key="what_1", value=True)
                where_1 = st.checkbox(FILTERS_INPUT["where_filter"], key="where_1", value=True)
                when_1 = st.checkbox(FILTERS_INPUT["when_filter"], key="when_1", value=True)
                submitted_1 = st.form_submit_button(FILTERS_INPUT['submit'])

        with col2_filters_2:
            st.markdown(FILTERS_INPUT['set_filters_2'])
            with st.form("filters_2"):
                ranking_2 = st.selectbox(label=FILTERS_INPUT["ranking"],
                                         options=["predicate", "entropy"],
                                         key="ranking_2")
                domain_range_2 = st.checkbox(FILTERS_INPUT["domain_range"], key="domain_range_2")
                who_2 = st.checkbox(FILTERS_INPUT["who_filter"], key="who_2")
                what_2 = st.checkbox(FILTERS_INPUT["what_filter"], key="what_2")
                where_2 = st.checkbox(FILTERS_INPUT["where_filter"], key="where_2")
                when_2 = st.checkbox(FILTERS_INPUT["when_filter"], key="when_2")
                submitted_2 = st.form_submit_button(FILTERS_INPUT["submit"])

        init_var([('param_1', False), ('param_2', False)])

        if submitted_1:
            st.session_state.param_1 = True

        if submitted_2:
            st.session_state.param_2 = True

        refresh = st.button("Refresh params", on_click=on_click_refresh_filters)

    if not (st.session_state.param_1 and st.session_state.param_2):
        st.warning(FILTERS_INPUT['no_submit_warning'])


# Container for graph search side
with st.container():
    st.write('#')
    st.markdown(GRAPH_SEARCH['headline'])

    if st.session_state["common_params"] and st.session_state['param_1'] and \
        st.session_state['param_2']:
        if check_variables_for_search():
            config_search_common = st.session_state.base_config[st.session_state.dataset]
            config_search_common.update(get_common_base_config())

            config_1, folder_1 = get_graph_search_info("1", config_search_common)
            config_2, folder_2 = get_graph_search_info("2", config_search_common)
            init_var([("folder_1", folder_1), ("folder_2", folder_2)])

            run_search = st.button(GRAPH_SEARCH['btn_run_search'])
            if run_search:
                with st.spinner("Running the search"):
                    for config, folder, nb in \
                        [(config_1, folder_1, "1"), (config_2, folder_2, "2")]:
                        start_time = datetime.now()
                        if (not os.path.exists(folder)) or \
                            (f"subgraph-{st.session_state['iterations']}.html" \
                                not in os.listdir(folder)):
                            if not os.path.exists(folder):
                                os.makedirs(folder)
                            run_search_save_info(config, folder)
                        end_time = datetime.now()
                        init_var([(f"time_exp_{nb}", end_time - start_time)])
                    st.session_state.experiments_run = True


# Container for displaying results - comparison
with st.container():
    st.write('#')
    st.markdown(RES_COMPARISON['headline'])
    st.markdown("#####")

    if st.session_state["experiments_run"]:

        col1_table_res, col2_table_res = st.columns(2)

        # Loading data to display results
        folder_1 = st.session_state["folder_1"]
        folder_2 = st.session_state["folder_2"]
        data_1 = read_pickled_data_graph_search(folder=folder_1)
        data_2 = read_pickled_data_graph_search(folder=folder_2)

        # Overall results
        with col1_table_res:
            st.write(RES_COMPARISON['filter_1'])
            st.write(RES_COMPARISON["time_exp"].format(st.session_state["time_exp_1"]))
            write_params("1")
            st.dataframe(data_1["path_expanded"])

        with col2_table_res:
            st.write(RES_COMPARISON['filter_2'])
            st.write(RES_COMPARISON["time_exp"].format(st.session_state["time_exp_2"]))
            write_params("2")
            st.dataframe(data_2["path_expanded"])


# Container for displaying results - at each iteration
with st.container():
    if st.session_state["experiments_run"]:
        st.markdown("##")
        # Results per iteration: metrics, html graph, path and nodes expanded
        ## Text content: different results
        st.markdown(RES_ITERATION['headline'])

        with st.expander(RES_ITERATION['which_results_expander']):
            st.markdown(RES_ITERATION['which_results_main'])
        iteration = st.slider("Iteration value", min_value=1, max_value=iterations,
                            step=1, label_visibility='hidden')

        col1_vis_graph, col2_vis_graph = st.columns(2)

        ## Metrics + Vis
        with col1_vis_graph:
            write_metrics(data=data_1["metrics"][iteration])
            write_params("1")
            source_code = get_source_code(html_path=f"{folder_1}/subgraph-{iteration}.html")
            components.html(source_code, width=750, height=750)

        with col2_vis_graph:
            write_metrics(data=data_2["metrics"][iteration])
            write_params("2")
            source_code = get_source_code(html_path=f"{folder_2}/subgraph-{iteration}.html")
            components.html(source_code, width=750, height=750)

        ## Path chosen + nodes expanded
        col1_node_path, col2_node_path = st.columns(2)

        with col1_node_path:
            with st.expander(RES_ITERATION["path_chosen"].format("at", iteration)):
                if 1 < iteration < st.session_state["iterations"]:
                    path = data_1["path_expanded"][data_1["path_expanded"] \
                    .index == iteration-1].path_expanded.values[0]
                    write_path_expanded(path)
                else:
                    st.write("N/A since first or last iteration")

            with st.expander(RES_ITERATION["node_expanded"].format(iteration)):
                nodes_expanded = data_2["nodes_expanded_per_iter"] \
                    [data_2["nodes_expanded_per_iter"].index == iteration] \
                        .node_expanded.values[0]
                write_nodes_expanded(nodes=nodes_expanded)

            with st.expander(RES_ITERATION["path_chosen"].format("for", iteration+1)):
                path = data_1["path_expanded"][data_1["path_expanded"] \
                    .index == iteration].path_expanded.values[0]
                write_path_expanded(path)

        with col2_node_path:
            with st.expander(RES_ITERATION["path_chosen"].format("at", iteration)):
                if 1 < iteration < st.session_state["iterations"]:
                    path = data_2["path_expanded"][data_2["path_expanded"] \
                    .index == iteration-1].path_expanded.values[0]
                    write_path_expanded(path)
                else:
                    st.write("N/A since first or last iteration")

            with st.expander(RES_ITERATION["node_expanded"].format(iteration)):
                nodes_expanded = data_2["nodes_expanded_per_iter"] \
                    [data_2["nodes_expanded_per_iter"].index == iteration] \
                        .node_expanded.values[0]
                write_nodes_expanded(nodes=nodes_expanded)

            with st.expander(RES_ITERATION["path_chosen"].format("for", iteration+1)):
                path = data_2["path_expanded"][data_1["path_expanded"] \
                    .index == iteration].path_expanded.values[0]
                write_path_expanded(path)
