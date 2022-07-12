""" Extracting directly information for domain/range on predicates """
import os
import json
import yaml
from collections import defaultdict
from tqdm import tqdm
from copy import deepcopy

from src.hdt_interface import HDTInterface
from settings import FOLDER_PATH

def get_params(config):
    """ Return parameters for the HDT interface """
    #filter_b
    if "exclude_category" in config:
        filter_kb = config["exclude_category"]
    else:
        filter_kb = 1

    #folder_hdt: directly accessible

    #dataset_config
    with open(
        os.path.join(FOLDER_PATH, "dataset-config", f"{config['dataset_type']}.yaml"),
                     encoding='utf-8') as file:
        dataset_config = yaml.load(file, Loader=yaml.FullLoader)

    #nested
    nested = config["nested_dataset"] if "nested_dataset" in config else 1

    #pred
    pred = dataset_config["point_in_time"] + dataset_config["start_dates"] + \
           dataset_config["end_dates"] + [dataset_config["rdf_type"]]

    return filter_kb, dataset_config, nested, pred


def get_triples(interface, params):
    """ Querying KB """
    return interface.run_request(
        params=params,
        filter_pred=[],
        filter_keep=False)


def get_type_wikidata(interface, type_to_extract, dataset_config):
    triples = get_triples(
        interface,
        params=dict(
            predicate=dataset_config["property_constraint_direct"],
            object=dataset_config[type_to_extract]
        ))
    predicates = set([x[0] for x in triples])

    statements = []
    pred_to_statement = {}
    for pred in predicates:
        triples = get_triples(
            interface,
            params=dict(
                predicate=dataset_config["property_constraint_prop"],
                subject=pred
            ))
        statements += [x[2] for x in triples]
        pred_to_statement[pred] = [x[2] for x in triples]

    filtered_sub_statements = []
    statement_to_class_type = {}
    for statement in set(statements):
        triples = get_triples(
            interface,
            params=dict(
                predicate=dataset_config["class_qualifier"],
                subject=statement
            ))
        filtered_sub_statements += triples
        statement_to_class_type[statement] = [x[2] for x in triples]

    # return pred to class type
    return {pred: list(set([x for statement in statements \
            for x in statement_to_class_type[statement]]))\
        for pred, statements in pred_to_statement.items()}


def get_superclass_wikidata(interface, dataset_config):
    triples = interface.run_request(
        params=dict(predicate=dataset_config["sub_class_of"][0]),
        filter_pred=[],
        filter_keep=False
    )
    class_to_sub_class = defaultdict(list)

    for triple in triples:
        class_to_sub_class[triple[0]].append(triple[2])
    
    output = deepcopy(class_to_sub_class)
    for k, sup_cl in class_to_sub_class.items():
        for node in [x for x in sup_cl if x in class_to_sub_class]:
            output[k] += deepcopy(class_to_sub_class[node])
    return {k: list(set(v)) for k, v in output.items()}

def extract_domain_range(config):
    """ Pre extracting info on constraints for predicates """
    filter_kb, dataset_config, nested, pred = get_params(config)
    interface = HDTInterface(filter_kb=filter_kb, folder_hdt=config["dataset_path"],
                             dataset_config=dataset_config, nested_dataset=nested,
                             default_pred=pred)

    if config["dataset_type"] == "dbpedia":
        triples = get_triples(interface=interface,
                              params=dict(predicate=dataset_config["domain"]))
        domain_pred = {x[0]: x[2] for x in triples}

        triples = get_triples(interface=interface,
                              params=dict(predicate=dataset_config["range"]))
        range_pred = {x[0]: x[2] for x in triples}

        superclasses = {}
        nodes = list(
            set([val for _, val in domain_pred.items()] + \
                [val for _, val in range_pred.items()])
        )
        for i in tqdm(range(len(nodes))):
            elt = nodes[i]
            superclasses[elt] = interface.get_superclass(node=elt)

        return domain_pred, range_pred, superclasses

    if config["dataset_type"] == "wikidata":
        domain_pred = get_type_wikidata(interface=interface,
            type_to_extract="domain", dataset_config=dataset_config)
        
        range_pred = get_type_wikidata(interface=interface,
            type_to_extract="range", dataset_config=dataset_config)

        superclasses = get_superclass_wikidata(interface=interface,
                                               dataset_config=dataset_config)
        return domain_pred, range_pred, superclasses

    return None, None, None


if __name__ == '__main__':
    # with open(os.path.join(FOLDER_PATH, "configs-example/config-dbpedia.json"),
    #           "r", encoding="utf-8") as openfile:
    #     CONFIG = json.load(openfile)
    # DOMAIN_PRED, RANGE_PRED, SUPERCLASSES = extract_domain_range(config=CONFIG)

    # with open(os.path.join(FOLDER_PATH, "domain-range-pred/dbpedia-domain.json"),
    #           "w", encoding="utf-8") as openfile:
    #     json.dump(DOMAIN_PRED, openfile)

    # with open(os.path.join(FOLDER_PATH, "domain-range-pred/dbpedia-range.json"),
    #           "w", encoding="utf-8") as openfile:
    #     json.dump(RANGE_PRED, openfile)

    # with open(os.path.join(FOLDER_PATH, "domain-range-pred/dbpedia-domain.json"),
    #           "w", encoding="utf-8") as openfile:
    #     json.dump(DOMAIN_PRED, openfile)

    # with open(os.path.join(FOLDER_PATH, "domain-range-pred/dbpedia-superclasses.json"),
    #           "w", encoding="utf-8") as openfile:
    #     json.dump(SUPERCLASSES, openfile)

    with open(os.path.join(FOLDER_PATH, "configs-example/config-wikidata.json"),
              "r", encoding="utf-8") as openfile:
        CONFIG = json.load(openfile)
    DOMAIN_PRED, RANGE_PRED, SUPERCLASSES = extract_domain_range(config=CONFIG)

    with open(os.path.join(FOLDER_PATH, "domain-range-pred/wikidata-domain.json"),
              "w", encoding="utf-8") as openfile:
        json.dump(DOMAIN_PRED, openfile)

    with open(os.path.join(FOLDER_PATH, "domain-range-pred/wikidata-range.json"),
              "w", encoding="utf-8") as openfile:
        json.dump(RANGE_PRED, openfile)
    
    with open(os.path.join(FOLDER_PATH, "domain-range-pred/wikidata-superclasses.json"),
              "w", encoding="utf-8") as openfile:
        json.dump(SUPERCLASSES, openfile)

