# -*- coding: utf-8 -*-
""" Dynamic content for the front-end text content """


LOGS_VARIABLES_SEARCH = {
    'start_node_empty': 'The starting should be non empty and start with ' + \
        '{} for dataset {}',
    'start_node_no_gs': 'The sub events of the starting node should be in a .csv file in ' + \
        'the folder {}gs_events, with name {}.csv',
    'start_node_no_ref': 'The referents of the starting node should be in a .json file in ' + \
        'the folder {}referents, with name {}.json',
    'start_date': 'start date should be in the format %Y-%m-%d',
    'end_date': 'end date should be in the format %Y-%m-%d',
}

BASE_CONFIG = {
    "Wikidata": {
        "rdf_type": {
            "conflict": "http://www.wikidata.org/entity/Q180684",
            "operation": "http://www.wikidata.org/entity/Q28972820",
            "cause_of_death": "http://www.wikidata.org/entity/Q1931388",
            "military_operation": "http://www.wikidata.org/entity/Q645883",
            "historical_event": "http://www.wikidata.org/entity/Q13418847",
            "combat": "http://www.wikidata.org/entity/Q650711",
            "battle": "http://www.wikidata.org/entity/Q178561"
        },
        "predicate_filter": [
            "http://www.wikidata.org/prop/direct/P910",
            "http://www.wikidata.org/prop/direct/P1343",
            "http://www.wikidata.org/prop/direct/P5008",
            "http://www.wikidata.org/prop/direct/P2184",
            "http://www.wikidata.org/prop/direct/P31"
        ],
        "type_interface": "hdt",
        "type_metrics": ["precision", "recall", "f1"],
        "dataset_type": "wikidata",
    },
    "DBpedia": {
        "rdf_type": {
            "event": "http://dbpedia.org/ontology/Event"
        },
        "predicate_filter": [
            "http://dbpedia.org/ontology/wikiPageWikiLink",
            "http://dbpedia.org/ontology/wikiPageRedirects",
            "http://dbpedia.org/ontology/wikiPageDisambiguates",
            "http://www.w3.org/2000/01/rdf-schema#seeAlso",
            "http://xmlns.com/foaf/0.1/depiction",
            "http://xmlns.com/foaf/0.1/isPrimaryTopicOf",
            "http://dbpedia.org/ontology/thumbnail",
            "http://dbpedia.org/ontology/wikiPageExternalLink",
            "http://dbpedia.org/ontology/wikiPageID",
            "http://dbpedia.org/ontology/wikiPageLength",
            "http://dbpedia.org/ontology/wikiPageRevisionID",
            "http://dbpedia.org/property/wikiPageUsesTemplate",
            "http://www.w3.org/2002/07/owl#sameAs",
            "http://www.w3.org/ns/prov#wasDerivedFrom",
            "http://dbpedia.org/ontology/wikiPageWikiLinkText",
            "http://dbpedia.org/ontology/wikiPageOutDegree",
            "http://dbpedia.org/ontology/abstract",
            "http://www.w3.org/2000/01/rdf-schema#comment",
            "http://www.w3.org/2000/01/rdf-schema#label"
        ],
        "type_interface": "hdt",
        "type_metrics": ["precision", "recall", "f1"],
        "dataset_type": "dbpedia",
    }
}

MAIN_LAYOUT = {
    "title": "Comparing systems for the narrative graph traversal",
}

EVENT_INPUT = {
    'headline': "### First enter the event you are interested in",
    'expand': "Expand to enter",
    'select_dataset': "Select a dataset",
    'select_start_node': "Select a starting node for the search",
    'select_start_date': "Enter a start date for the starting node",
    'select_end_date': "Enter a start date for the starting node",
    'stop_param': 'Choose parameter to stop the search',
    'stop_iteration': 'Iteration number',
    'stop_uri_limit': 'Number of URIs to expand',
    'submit_stop_param': 'Continue',
    'no_submit_max_uri': 'You need to choose one option for the max URIs,' + \
        ' and then click on Continue',
    'iterations': 'Choose a number of iterations',
    'max_uri': 'Do you want to set a maximum number of URIs to expand at each iteration?',
    'max_uri_val': "Pick a number of max URIs to expand at each iteration",
    'submit': 'Submit',
    'refresh_common_params': "Refresh common params",
    'refresh_system_params': "Refresh system params",
    'no_submit_warning': "You need to enter an event information to compare the" + \
        " set of filters. Please expand to enter the input information and ' + \
            'click 'Continue', then 'Submit'."
}

SYSTEM_INPUT = {
    'headline': "### Then select the filters you want to compare",
    'expand': "Expand to select",
    'set_filters_1': "##### Set of filters 1",
    'filters_prune_search_space': 'Choose any of the filters to prune the search space',
    'who_filter': 'who',
    'what_filter': 'what',
    'where_filter': 'where',
    'when_filter': 'when',
    'expand_all_vs_subset': 'Do you want to expand all nodes, a random subset' + \
        ' or an informed subset (for each iteration)?',
    'nb_random': 'Choose a number of nodes to be randomly chosen at each iteration',
    'ranking': 'ranking',
    'domain_range': 'domain/range',
    'ranking_metrics': [
        'pred_freq', 'entropy_pred_freq', 'inverse_pred_freq',
        'pred_object_freq', 'entropy_pred_object_freq', 'inverse_pred_object_freq'
    ],
    'submit': 'Submit',
    'set_filters_2': "##### Set of filters 2",
    'no_submit_warning': "You need to select two sets of filters to compare. Please" + \
        " expand to select the filters and click 'Continue' & 'Submit' for each set of filters.",
    'continue': "Continue",
}

GRAPH_SEARCH = {
    'headline': "### Run the search in the graph",
    'btn_run_search': 'Run the search',
    'pending_run_search': "Running the search",

}

RES_COMPARISON = {
    'headline': "### Comparing the two set of parameters",
    'filter_1': "#### Set of filters - 1",
    'filter_2': "#### Set of filters - 2",
    'time_exp': "Took {} to run."
}

RES_ITERATION = {
    'headline': "#### Pick an iteration value",
    'path_chosen': "Path {} iteration {}",
    'node_expanded': "Nodes expanded at iteration {}",
    'which_results_expander': "What are the results that I can see?",
    'which_results_main': """
        #####
        Below you can find results for each iteration `i` and set of filters: 
        * Metrics (precision, recall, f1)
        * The subgraph extracted by the framework:
            * Green nodes are true positives,
            * Red nodes are false positives,
            * Blue nodes are nodes added at iteration `i`, and that are part of the graph
            * Grey nodes are nearly identical to blue nodes, but they were added in a previous iteration
        * If you are using a `subset-informed` system (during the expansion phase, heuristic to prioritise nodes)
            * Path chosen and nodes expanded at iteration `i`: nodes that were expanded at iteration `i` and who correspond to the path chosen
            * Path chosen for iteration `i+1`: path with the highest score at the end of iteration `i`
        * Else
            * Nodes expanded at each iteration, and the triple in which they were viisted
        
        ####

        For the example below we use the abbreviations
        
        ``` 
        dbo: http://dbpedia.org/ontology/
        dbr: http://dbpedia.org/resource/
        ```

        Imagine you have the following path chosen at iteration `i`:
        ```
        type: ingoing
        predicate: dbo:isPartOfMilitaryConflict
        object: dbr:French_Revolution
        ```

        and the following expanded nodes at that same iteration `i`:
        ```
        dbr:Battle_of_Kaiserslautern
        dbr:Battle_of_Wattignies
        ```
        
        It means that you have the triples below in your graph:
        ```
        (dbr:Battle_of_Kaiserslautern, dbo:isPartOfMilitaryConflict, dbr:French_Revolution)
        (dbr:Battle_of_Wattignies, dbo:isPartOfMilitaryConflict, dbr:French_Revolution)
        ```

    """
}
