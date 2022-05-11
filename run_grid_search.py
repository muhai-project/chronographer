""" Running grid search for one event"""
import os
import json
import psutil
from copy import deepcopy
from sklearn.model_selection import ParameterGrid
from ray.util.multiprocessing.pool import Pool

from src.framework import GraphSearchFramework
from settings import FOLDER_PATH


base_predicates = [
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
]

param_grid = {
    "predicate_filter": [
        base_predicates,
        base_predicates + ["http://dbpedia.org/ontology/wikiPageWikiLink"]
    ],

    "type_ranking": ["pred_object_freq", "entropy_pred_object_freq"],

    "ordering_domain_range": [0, 1],
    "filtering_when": [0, 1],
    "exclude_category": [0, 1]
}


def update_config(config, args):
    """ Updating config for sweep (inline params) """
    config = deepcopy(config)
    config['type_ranking'] = args['type_ranking']
    if "ordering" not in config:
        config["ordering"] = {}
    config["ordering"]["domain_range"] = int(args['ordering_domain_range'])
    config["predicate_filter"] = args["predicate_filter"]

    config["filtering"] = {"what": 1, "where": 1}
    if args['filtering_when'] is not None:
        config["filtering"]["when"] = int(args['filtering_when'])

    return config

def run_framework(config):
    """ Calling graph search framework """
    config["rdf_type"] = list(config["rdf_type"].items())
    framework = GraphSearchFramework(config=config)
    framework()

params = list(ParameterGrid(param_grid))
params = [elt for elt in params if elt.get('ordering_domain_range') or elt.get('filtering_when')]


with open(os.path.join(FOLDER_PATH, "data-test/config/French_Revolution.json"),
          "r", encoding="utf-8") as openfile:
    CONFIG = json.load(openfile)
args_grid = [update_config(CONFIG, param) for param in params]


def main():
    pool = Pool(processes=psutil.cpu_count(logical=False))
    pool.map(run_framework, args_grid)
    pool.close()

if __name__ == '__main__':
    main()
