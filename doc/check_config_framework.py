# -*- coding: utf-8 -*-
"""
Storing type error messages when checking config for frameworks
"""

CONFIG_TYPE_ERROR_MESSAGES = {
    "rdf_type": "`rdf_type` should be in the config keys" + \
        "Format: dict (k, v) (str, str)",

    "predicate_filter": "`predicate_filter` should be in the config keys " + \
        "Format: list of strings, each str URI of predicate to discard",

    "start": "`start` should be in the config keys" + \
        "Format: string URI of node to begin the search with",

    "iterations": "`iterations` should be in the config keys" + \
        "Format: int, number of iterations to run",

    "type_ranking": "`type_ranking` should be in the config keys" + \
        "Format: must be one of the followings (str): " + \
        "`pred_freq`, `inverse_pred_freq`, `entropy_pred_freq`, " + \
        "`pred_object_freq`, `inverse_pred_object_freq`, `entropy_pred_object_freq`",

    "type_interface": "`type_interface` should be in the config keys" + \
        "Format: must be str `hdt` ",

    "gold_standard": "`gold_standard` should be in the config keys" + \
        "Format: relative path of .csv file containing gold standard events",

    "referents": "`referents` should be in the config keys" + \
        "Format: relative path of .json file containing correct referent for former urls" + \
            "key and value are strings",

    "type_metrics": "`type_metrics` should be in the config keys" + \
        "Format: list[str], contain at most `precision`, `recall` and `f1`",

    "start_date": "`start_date` should be in the config keys" + \
        "Format: str, YYYY-MM-DD",

    "end_date": "`end_date` should be in the config keys" + \
        "Format: str, YYYY-MM-DD",

    "ordering": {
        "domain_range": "Ordering parameter for domain_range should be 1 or 0 (default 0)"
    },

    "filtering": {
        w_filter: "Filtering parameter for what should be 1 or 0 (default 0)" \
            for w_filter in ["what", "where", "when"]
    },

    "name_exp": "`name_exp` should be in the config keys" + \
        "Format: str",

    "dataset_type": "`dataset_type` should be either `dbpedia`, `wikidata` or `yago`",
    "dataset_path": "`dataset_path` should be of type string",
    "sparql_endpoint": "`sparql_endpoint` should be of type string",
    "uri_limit": "`uri_limit` should be in the config keys if type_walk is `random`, " + \
        "with format: int, or with value `all`",
    "max_uri": "`max_uri`, if used, should be an integer"

}
