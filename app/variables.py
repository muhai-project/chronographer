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