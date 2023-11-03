# -*- coding: utf-8 -*-
""" Variables to change for the interface to run
(access to dataset info: dataset path and ground truth/referents directions)"""

VARIABLES_DATASET = {
    "Wikidata": {
        "dataset_path": "../wikidata-2021-03-05/",
        "data_files_path": "../data-test/wikidata/",
        "start_uri": "http://www.wikidata.org/entity/",
        "nested_dataset": 0
    },
    "DBpedia": {
        "dataset_path": "../dbpedia-snapshot-2021-09/",
        "data_files_path": "../data-test/dbpedia/",
        "start_uri": "http://dbpedia.org/resource/",
        "nested_dataset": 1
    }
}

DEFAULT_VARIABLES = {
    # common params
    "dataset": sorted(tuple(VARIABLES_DATASET.keys()))[0],
    "start_node": "http://dbpedia.org/resource/French_Revolution",
    "start_date": "1789-05-05",
    "end_date": "1799-12-31",
    "iterations": 5,
    "max_uri": "",
    "max_uri_val": "all",
    # system params
    # if key `k_` ends with `_`, means that there is `k_1` and `k_2`
    # for each of the two systems that are being compared
    "filters_": [],
    "expand_all_vs_subset_": "",
    "nb_random_": 15,
    "walk_": "random",
    "ranking_": "pred_freq",
    "domain_range_": False,
}
