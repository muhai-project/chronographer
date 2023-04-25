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
    on_click_set_true, \
    on_click_refresh_common_params, \
        write_params, write_nodes_expanded, write_path_expanded, \
        write_metrics
from utils.read_data import read_pickled_data_graph_search, get_source_code

from variables import VARIABLES_DATASET
from content import LOGS_VARIABLES_SEARCH, BASE_CONFIG, EVENT_INPUT, \
    SYSTEM_INPUT, GRAPH_SEARCH, RES_COMPARISON, RES_ITERATION


# Setting layout + init session state varialbes
st.set_page_config(layout="wide")
init_var(var_list=[("variables_dataset", VARIABLES_DATASET),
                   ("logs_variables_search", LOGS_VARIABLES_SEARCH),
                   ("base_config", BASE_CONFIG),
                   ("max_uri_val", "all")])
init_var(var_list=[(var, False) for var in \
    ["experiments_run", "common_params", 'submit_all_vs_subset_1',
     'submit_all_vs_subset_2', 'param_1', 'param_2', 'stop_param_val',
     'submit_max_uri']])
init_var(var_list=[(f"{var}_{id_var}", 0) for var in  \
    ["nb_random", "domain_range"] \
        for id_var in ["1", "2"]])
init_var(var_list=[(f"{var}_{id_var}", "") for var in  \
    ["ranking"] \
        for id_var in ["1", "2"]])
init_var(var_list=[('walk_1', 'random'), ('walk_2', 'random')])

st.title("Comparing systems for the narrative graph traversal")

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
                                         max_value=100, value=5, step=1, key="iterations")

            max_uri = st.selectbox(label=EVENT_INPUT['max_uri'], options=["", "Yes", "No"],
                                   key="max_uri")
            submit_max_uri = st.form_submit_button(EVENT_INPUT['submit_stop_param'],
                                                   on_click=on_click_set_true('submit_max_uri'))

        if st.session_state.submit_max_uri and max_uri != '':
            submit_common_params = False
            if max_uri == "Yes":
                with st.form('max_uri_form'):
                    max_uri_val = st.number_input(label=EVENT_INPUT['max_uri_val'],  min_value=1,
                                            max_value=1000, value=50, step=1, key="max_uri_val")
                    init_var([('max_uri_val', max_uri_val)])
                    submit_common_params = st.form_submit_button(EVENT_INPUT['submit'])

            if submit_common_params or max_uri == 'No':
                st.session_state["common_params"] = True
        if not st.session_state.common_params:
            st.warning(EVENT_INPUT['no_submit_max_uri'])

        refresh_common_params = st.button(EVENT_INPUT['refresh'],
                                          on_click=on_click_refresh_common_params)

    if not st.session_state.common_params:
        st.warning(EVENT_INPUT['no_submit_warning'])
    else:
        st.empty()

# Container for system selection
with st.container():
    st.markdown(SYSTEM_INPUT['headline'])

    with st.expander(SYSTEM_INPUT['expand']):
        col1_filters_1, col2_filters_2 = st.columns(2)

        with col1_filters_1:
            st.markdown(SYSTEM_INPUT['set_filters_1'])

            with st.form("filters_1"):
                filters_1 = st.multiselect(
                SYSTEM_INPUT['filters_prune_search_space'],
                [SYSTEM_INPUT["who_filter"], SYSTEM_INPUT["what_filter"],
                SYSTEM_INPUT["where_filter"], SYSTEM_INPUT["when_filter"]],
                key="filters_1")
                init_var([('filters_1', filters_1)])

                expand_all_vs_subset_1 = st.selectbox(
                    SYSTEM_INPUT['expand_all_vs_subset'],
                    ["", "all", "subset-random", "subset-informed"])
                submit_expand_all_vs_subset_1 = st.form_submit_button(
                    SYSTEM_INPUT['continue'],
                    on_click=on_click_set_true('submit_all_vs_subset_1'))

            if st.session_state.submit_all_vs_subset_1 and expand_all_vs_subset_1 != "":
                submitted_1 = False
                if 'subset' in expand_all_vs_subset_1:
                    with st.form("subset_1"):
                        if expand_all_vs_subset_1 == 'subset-random':
                            nb_random_1 = st.number_input(
                                label=SYSTEM_INPUT['nb_random'], min_value=1,
                                max_value=100, value=15, step=1, key="nb_random_1")
                            init_var([('nb_random_1', nb_random_1)])
                        if expand_all_vs_subset_1 == 'subset-informed':
                            st.session_state.walk_1 = "informed"
                            ranking_1 = st.selectbox(label=SYSTEM_INPUT["ranking"],
                                                options=SYSTEM_INPUT['ranking_metrics'],
                                                key="ranking_1")
                            init_var([('ranking_1', ranking_1)])
                            domain_range_1 = st.checkbox(SYSTEM_INPUT["domain_range"],
                                                        key="domain_range_1", value=True)
                            init_var([('domain_range_1', domain_range_1)])

                        submitted_1 = st.form_submit_button(SYSTEM_INPUT['submit'])

                if submitted_1 or expand_all_vs_subset_1 == "all":
                    st.session_state["param_1"] = True

        with col2_filters_2:
            st.markdown(SYSTEM_INPUT['set_filters_2'])

            with st.form("filters_2"):
                filters_2 = st.multiselect(
                SYSTEM_INPUT['filters_prune_search_space'],
                [SYSTEM_INPUT["who_filter"], SYSTEM_INPUT["what_filter"],
                SYSTEM_INPUT["where_filter"], SYSTEM_INPUT["when_filter"]],
                key="filters_2")
                init_var([('filters_2', filters_2)])

                expand_all_vs_subset_2 = st.selectbox(
                    SYSTEM_INPUT['expand_all_vs_subset'],
                    ["", "all", "subset-random", "subset-informed"])
                submit_expand_all_vs_subset_2 = st.form_submit_button(
                    SYSTEM_INPUT['continue'],
                    on_click=on_click_set_true('submit_all_vs_subset_2'))

            if st.session_state.submit_all_vs_subset_2 and expand_all_vs_subset_2 != "":
                submitted_2 = False
                if 'subset' in expand_all_vs_subset_2:
                    with st.form("subset_2"):
                        if expand_all_vs_subset_2 == 'subset-random':
                            nb_random_2 = st.number_input(
                                label=SYSTEM_INPUT['nb_random'], min_value=1,
                                max_value=100, value=15, step=1, key="nb_random_2")
                            init_var([('nb_random_2', nb_random_2)])
                        if expand_all_vs_subset_2 == 'subset-informed':
                            init_var([('walk_2', 'informed')])
                            ranking_2 = st.selectbox(label=SYSTEM_INPUT["ranking"],
                                                options=SYSTEM_INPUT['ranking_metrics'],
                                                key="ranking_2")
                            init_var([('ranking_2', ranking_2)])
                            domain_range_2 = st.checkbox(SYSTEM_INPUT["domain_range"],
                                                        key="domain_range_2", value=True)
                            init_var([('domain_range_2', domain_range_2)])

                        submitted_2 = st.form_submit_button(SYSTEM_INPUT['submit'])

                if submitted_2 or expand_all_vs_subset_2 == "all":
                    st.session_state["param_2"] = True

        refresh = st.button("Refresh params", on_click=on_click_refresh_filters)

    if not (st.session_state.param_1 and st.session_state.param_2):
        st.warning(SYSTEM_INPUT['no_submit_warning'])



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

            st.write(config_1)
            st.write(config_2)
            st.write(st.session_state)
            init_var([("folder_1", folder_1), ("folder_2", folder_2)])
            st.write(folder_1, folder_2)

            run_search = st.button(GRAPH_SEARCH['btn_run_search'])
            if run_search:
                with st.spinner("Running the search"):
                    for config, folder, walk, nb in [(config_1, folder_1, st.session_state.walk_1, "1"), (config_2, folder_2, st.session_state.walk_2,  "2")]:
                        start_time = datetime.now()
                        if (not os.path.exists(folder)) or \
                            (f"subgraph-{st.session_state['iterations']}.html" \
                                not in os.listdir(folder)):
                            if not os.path.exists(folder):
                                os.makedirs(folder)
                            run_search_save_info(config, folder, walk)
                        end_time = datetime.now()
                        init_var([(f"time_exp_{nb}", end_time - start_time)])
                    st.session_state.experiments_run = True


# # Container for displaying results - comparison
# with st.container():
#     st.write('#')
#     st.markdown(RES_COMPARISON['headline'])
#     st.markdown("#####")

#     if st.session_state["experiments_run"]:

#         col1_table_res, col2_table_res = st.columns(2)

#         # Loading data to display results
#         folder_1 = st.session_state["folder_1"]
#         folder_2 = st.session_state["folder_2"]
#         data_1 = read_pickled_data_graph_search(folder=folder_1)
#         data_2 = read_pickled_data_graph_search(folder=folder_2)

#         # Overall results
#         with col1_table_res:
#             st.write(RES_COMPARISON['filter_1'])
#             st.write(RES_COMPARISON["time_exp"].format(st.session_state["time_exp_1"]))
#             write_params("1")
#             st.dataframe(data_1["path_expanded"])

#         with col2_table_res:
#             st.write(RES_COMPARISON['filter_2'])
#             st.write(RES_COMPARISON["time_exp"].format(st.session_state["time_exp_2"]))
#             write_params("2")
#             st.dataframe(data_2["path_expanded"])


# # Container for displaying results - at each iteration
# with st.container():
#     if st.session_state["experiments_run"]:
#         st.markdown("##")
#         # Results per iteration: metrics, html graph, path and nodes expanded
#         ## Text content: different results
#         st.markdown(RES_ITERATION['headline'])

#         with st.expander(RES_ITERATION['which_results_expander']):
#             st.markdown(RES_ITERATION['which_results_main'])
#         # TODO: change max value
#         iteration = st.slider("Iteration value", min_value=1, max_value=15,
#                             step=1, label_visibility='hidden')

#         col1_vis_graph, col2_vis_graph = st.columns(2)

#         ## Metrics + Vis
#         with col1_vis_graph:
#             write_metrics(data=data_1["metrics"][iteration])
#             write_params("1")
#             source_code = get_source_code(html_path=f"{folder_1}/subgraph-{iteration}.html")
#             components.html(source_code, width=750, height=750)

#         with col2_vis_graph:
#             write_metrics(data=data_2["metrics"][iteration])
#             write_params("2")
#             source_code = get_source_code(html_path=f"{folder_2}/subgraph-{iteration}.html")
#             components.html(source_code, width=750, height=750)

#         ## Path chosen + nodes expanded
#         col1_node_path, col2_node_path = st.columns(2)

#         with col1_node_path:
#             with st.expander(RES_ITERATION["path_chosen"].format("at", iteration)):
#                 if 1 < iteration < st.session_state["iterations"]:
#                     path = data_1["path_expanded"][data_1["path_expanded"] \
#                     .index == iteration-1].path_expanded.values[0]
#                     write_path_expanded(path)
#                 else:
#                     st.write("N/A since first or last iteration")

#             with st.expander(RES_ITERATION["node_expanded"].format(iteration)):
#                 nodes_expanded = data_2["nodes_expanded_per_iter"] \
#                     [data_2["nodes_expanded_per_iter"].index == iteration] \
#                         .node_expanded.values[0]
#                 write_nodes_expanded(nodes=nodes_expanded)

#             with st.expander(RES_ITERATION["path_chosen"].format("for", iteration+1)):
#                 path = data_1["path_expanded"][data_1["path_expanded"] \
#                     .index == iteration].path_expanded.values[0]
#                 write_path_expanded(path)

#         with col2_node_path:
#             with st.expander(RES_ITERATION["path_chosen"].format("at", iteration)):
#                 if 1 < iteration < st.session_state["iterations"]:
#                     path = data_2["path_expanded"][data_2["path_expanded"] \
#                     .index == iteration-1].path_expanded.values[0]
#                     write_path_expanded(path)
#                 else:
#                     st.write("N/A since first or last iteration")

#             with st.expander(RES_ITERATION["node_expanded"].format(iteration)):
#                 nodes_expanded = data_2["nodes_expanded_per_iter"] \
#                     [data_2["nodes_expanded_per_iter"].index == iteration] \
#                         .node_expanded.values[0]
#                 write_nodes_expanded(nodes=nodes_expanded)

#             with st.expander(RES_ITERATION["path_chosen"].format("for", iteration+1)):
#                 path = data_2["path_expanded"][data_1["path_expanded"] \
#                     .index == iteration].path_expanded.values[0]
#                 write_path_expanded(path)
