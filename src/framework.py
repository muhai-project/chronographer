"""
#TO DO: add documentation on this script
"""
import os
import json
import multiprocessing as mp
from datetime import datetime
from collections import defaultdict
from ray.util.multiprocessing import Pool

import pandas as pd
from rdflib.term import URIRef
from settings import FOLDER_PATH
from src.ranker import Ranker
from src.metrics import Metrics
from src.plotter import Plotter
from src.ordering import Ordering
from src.expansion import NodeExpansion
from src.hdt_interface import HDTInterface
from src.triply_interface import TriplInterface
from doc.check_config_framework import CONFIG_TYPE_ERROR_MESSAGES \
    as config_error_messages


CONFIG = {
    # "rdf_type": [("event", URIRef("http://dbpedia.org/ontology/Event")),
    #              ("person", URIRef("http://dbpedia.org/ontology/Person"))],
    "rdf_type": [("event", URIRef("http://dbpedia.org/ontology/Event"))],
    "predicate_filter": ["http://dbpedia.org/ontology/wikiPageWikiLink",
                         "http://dbpedia.org/ontology/wikiPageRedirects"],
    "start": "http://dbpedia.org/resource/Category:French_Revolution",
    "iterations": 0,
    "type_ranking": "entropy_pred_object_freq",
    "type_interface": "triply",
}

class GraphSearchFramework:
    """
    #TO DO: add documentation on this script
    """
    def __init__(self, config: dict):
        """
        Type of ranking strategies implemented:
            - pred_freq:
            - entropy_pred_freq:
            - inverse_pred_freq:
            - pred_object_freq:
            - entropy_pred_object_freq:
            - inverse_pred_object_freq:
            - not implemented: subject_freq:
            - not implemented: inverse_subject_freq:
            - not implemented: inverse_pred_object_split_freq:
        """
        self.possible_type_interface = ["triply", "hdt"]
        self.possible_type_ranking = [
            "pred_freq", "inverse_pred_freq", "entropy_pred_freq",
            "pred_object_freq", "inverse_pred_object_freq", "entropy_pred_object_freq"]
        self.possible_type_metrics = ["precision", "recall", "f1"]
        self.config_error_messages = config_error_messages

        self._check_config(config=config)
        self.config = config
        self.rdf_type = config["rdf_type"]
        self.predicate_filter = config["predicate_filter"]
        self.start = config["start"]
        self.iterations = config["iterations"]
        self.type_ranking = config["type_ranking"]

        self.dates = [config["start_date"], config["end_date"]]

        self.type_interface = config["type_interface"]
        if self.type_interface == "triply":
            self.interface = TriplInterface()
        else:  # type_interface == "triply"
            self.interface = HDTInterface()

        self.subgraph = pd.DataFrame(columns=["subject", "predicate", "object"])

        self.pending_nodes_ingoing = pd.DataFrame(columns=["subject", "predicate", "object"])
        self.pending_nodes_outgoing = pd.DataFrame(columns=["subject", "predicate", "object"])
        self.info = pd.DataFrame(columns=["path", "iteration", "tot"] + \
            [x for elt in self.rdf_type for x in [f"{elt[0]}"] ])

        self.nb_cpu = mp.cpu_count()
        self.paths = []

        self.ranker = Ranker(type_ranking=self.type_ranking)
        self.nodes_expanded = []
        self.occurence = defaultdict(int)
        self.to_expand = None
        self.expanded = {}

        self.metrics = Metrics()
        self.type_metrics = config["type_metrics"]
        df_gs = pd.read_csv(config['gold_standard'])
        self.event_gs = list(df_gs[df_gs['linkDBpediaEn']!=''].linkDBpediaEn.unique())
        self.metrics_data = {}

        self.plotter = Plotter()

        self.folder_name_suffix = \
            f"iter-{self.iterations}-{self.type_interface}-{self.type_ranking}"
        self.config = config


        ordering_domain_range = config["ordering"]["domain-range"] if \
            "ordering" in config and "domain-range" in config["ordering"] else 0
        self.ordering = Ordering(domain_range=ordering_domain_range)

        if "filtering" in config and "what" in config["filtering"] and \
            config["filtering"]["what"]:
            self.predicate_filter += ["http://www.w3.org/1999/02/22-rdf-syntax-ns#type"]

        filtering_when = config["filtering"]["when"] if \
            "fitlering" in config and "when" in config["filtering"] else 0
        filtering_where = config["filtering"]["where"] if \
            "fitlering" in config and "where" in config["filtering"] else 0

        self.node_expander = NodeExpansion(rdf_type=self.rdf_type,
                                           interface=self.interface,
                                           args_filtering={"when": filtering_when,
                                                           "where": filtering_where})

    def _check_config(self, config: dict):
        if not isinstance(config, dict):
            raise TypeError("`config` param type should be dict`")

        if "rdf_type" not in config:
            raise ValueError(self.config_error_messages['rdf_type'])
        if not isinstance(config["rdf_type"], list) or \
            any(not isinstance(elt, tuple) for elt in config["rdf_type"]) or \
            any(not isinstance(k, str) \
            or not isinstance(v, URIRef) for k, v in config['rdf_type']):
            raise TypeError(self.config_error_messages['rdf_type'])

        if "predicate_filter" not in config:
            raise ValueError(self.config_error_messages['predicate_filter'])
        if not isinstance(config["predicate_filter"], list) or \
            any(not isinstance(elt, str) for elt in config["predicate_filter"]):
            raise TypeError(self.config_error_messages['predicate_filter'])

        if "start" not in config:
            raise ValueError(self.config_error_messages['start'])
        if not isinstance(config["start"], str):
            raise TypeError(self.config_error_messages['start'])

        if "iterations" not in config:
            raise ValueError(self.config_error_messages['iterations'])
        if not isinstance(config["iterations"], int):
            raise TypeError(self.config_error_messages['iterations'])

        if "type_ranking" not in config:
            raise ValueError(self.config_error_messages['type_ranking'])
        if config["type_ranking"] not in self.possible_type_ranking:
            raise TypeError(self.config_error_messages['type_ranking'])

        if "type_interface" not in config:
            raise ValueError(self.config_error_messages['type_interface'])
        if config["type_interface"] not in self.possible_type_interface:
            raise TypeError(self.config_error_messages['type_interface'])

        if "gold_standard" not in config:
            raise ValueError(self.config_error_messages['gold_standard'])
        try:
            pd.read_csv(config["gold_standard"])[['startTime', 'callret-1', 'linkDBpediaEn']]
        except Exception as type_error:
            raise TypeError(self.config_error_messages['gold_standard']) from type_error

        if "type_metrics" not in config:
            raise ValueError(self.config_error_messages['type_metrics'])
        if not isinstance(config['type_metrics'], list) or \
            any(elt not in self.possible_type_metrics for elt in config['type_metrics']):
            raise TypeError(self.config_error_messages['type_metrics'])

        for date in ["start_date", "end_date"]:
            if date not in config:
                raise ValueError(self.config_error_messages[date])
            try:
                datetime(int(config[date][:4]), int(config[date][5:7]), int(config[date][8:10]))
            except Exception as type_error:
                raise TypeError(self.config_error_messages[date]) from type_error

        for k_p, v_p in [
            ("ordering", "domain-range"), ("filtering", "what"),
            ("filtering", "when"), ("filtering", "where")
        ]:

            if k_p in config and \
                isinstance(config[k_p], dict) and v_p in config[k_p]:
                if config[k_p][v_p] not in [0, 1]:
                    raise TypeError(self.config_error_messages[k_p][v_p])

    def select_nodes_to_expand(self):
        """ Accessible call to _select_nodes_to_expand"""
        return self._select_nodes_to_expand()

    def _select_nodes_to_expand(self):
        if self.to_expand:
            self.to_expand = self.to_expand[2:]
            path = [self.to_expand]

            # Gettings args for next iteration
            if (";" in self.to_expand) and ("ingoing" in self.to_expand):
                pred, obj = self.to_expand.replace('ingoing-', '').split(";")
                pred, obj = URIRef(pred), URIRef(obj)
                nodes = list(
                    self.pending_nodes_ingoing[
                        (self.pending_nodes_ingoing.predicate.isin([pred, str(pred)])) & \
                        (self.pending_nodes_ingoing.object.isin([obj, str(obj)]))].subject.values)
            elif (";" in self.to_expand) and ("outgoing" in self.to_expand):
                subj, pred = self.to_expand.replace('outgoing-', '').split(";")
                subj, pred = URIRef(subj), URIRef(pred)
                nodes = list(
                    self.pending_nodes_outgoing[
                        (self.pending_nodes_outgoing.predicate.isin([pred, str(pred)])) & \
                        (self.pending_nodes_outgoing.subject \
                            .isin([subj, str(subj)]))].object.values)
            else:
                print(self.pending_nodes_ingoing.predicate.unique())
                nodes = list(self.pending_nodes_ingoing[\
                    self.pending_nodes_ingoing.predicate.isin(
                        [self.to_expand, URIRef(self.to_expand)])].subject.values) + \
                        list(self.pending_nodes_outgoing[\
                    self.pending_nodes_outgoing.predicate.isin(
                        [self.to_expand, URIRef(self.to_expand)])].object.values)
                print(nodes)

        else:  # INIT state: only starting node
            path, nodes = [], [self.start]

        return [node for node in nodes if node not in self.nodes_expanded], path

    def _expand_one_node(self, args: dict):
        return self.node_expander(args=args, dates=self.dates)

    def _run_one_iteration(self, iteration: int):
        nodes_to_expand, path = self._select_nodes_to_expand()

        pool = Pool(self.nb_cpu)
        output = pool.map(self._expand_one_node,
                            [{
                                "node": node,
                                "path": path,
                                "predicate": self.predicate_filter,
                                "iteration": iteration
                                } for node in nodes_to_expand])
        pool.close()
        pool.join()

        return output

    def update_occurence(self, ingoing: pd.core.frame.DataFrame,
                         outgoing: pd.core.frame.DataFrame, occurence: dict):
        """ Accessible call to _update_occurence """
        return self._update_occurence(ingoing, outgoing, occurence)

    @staticmethod
    def _get_nb(superclass, pred):
        if superclass:
            return "1"
        if pred in []:
            return "2"
        return "3"

    def _update_occurence(self, ingoing: pd.core.frame.DataFrame,
                          outgoing: pd.core.frame.DataFrame, occurence: dict):
        """
        Updating occurences for future path ranking
        In any case: adding info about the type of predicate
        1 = relevant superclass | 2 = filtered predicate | 3 = other
        - If path on predicate only: {1-3} + pred
        - If pred_object: adding whether ingoing or outgoing
        """
        if self.type_ranking in ["pred_freq", "entropy_pred_freq",
                                 "inverse_pred_freq"]:  # predicate
            for _, row in ingoing.iterrows():
                nb_order = self._get_nb(superclass=row.superclass, pred=row.predicate)
                occurence[f"{nb_order}-{str(row.predicate)}"] += 1
            for _, row in outgoing.iterrows():
                nb_order = self._get_nb(superclass=row.superclass, pred=row.predicate)
                occurence[f"{nb_order}-{str(row.predicate)}"] += 1
        if self.type_ranking in ["pred_object_freq",
                                 "entropy_pred_object_freq",
                                 "inverse_pred_object_freq"]:  # subject predicate
            for _, row in ingoing.iterrows():
                nb_order = self._get_nb(superclass=row.superclass, pred=row.predicate)
                occurence[f"{nb_order}-ingoing-{str(row.predicate)};{str(row.object)}"] += 1
            for _, row in outgoing.iterrows():
                nb_order = self._get_nb(superclass=row.superclass, pred=row.predicate)
                occurence[f"{nb_order}-outgoing-{str(row.subject)};{str(row.predicate)}"] += 1
        return occurence


    def _merge_outputs(self, output: list, iteration: int, info: dict):
        for subgraph_ingoing, path_ingoing, subgraph_outgoing, path_outgoing, _ in output:
            self._merge_outputs_single_run(subgraph_ingoing, path_ingoing,
                                           subgraph_outgoing, path_outgoing, info, iteration)

        self.to_expand = self.ranker(occurences=self.occurence)
        if self.to_expand:
            self.occurence = defaultdict(int, {k:v for k, v in self.occurence.items() \
                if k != self.to_expand})
            self.pending_nodes_ingoing = self.pending_nodes_ingoing[\
                ~self.pending_nodes_ingoing.subject.isin(self.nodes_expanded)]
            self.pending_nodes_outgoing = self.pending_nodes_outgoing[\
                ~self.pending_nodes_outgoing.object.isin(self.nodes_expanded)]

        return info

    def _merge_outputs_single_run(self, subgraph_ingoing: pd.core.frame.DataFrame,
                                  path_ingoing: pd.core.frame.DataFrame,
                                  subgraph_outgoing: pd.core.frame.DataFrame,
                                  path_outgoing: pd.core.frame.DataFrame,
                                  info: dict, iteration: int):
        self.subgraph = pd.concat([self.subgraph, subgraph_ingoing], axis=0)
        self.subgraph = pd.concat([self.subgraph, subgraph_outgoing], axis=0)

        # Pre-ordering step (remove non relevant predicates)
        # 1st = add info on predicates (using domain/range information)
        path_ingoing, info = self.ordering(triple_df=path_ingoing, type_node="ingoing",
                                            info=info, iteration=iteration)
        path_outgoing, info = self.ordering(triple_df=path_outgoing, type_node="outgoing",
                                            info=info, iteration=iteration)

        self.pending_nodes_ingoing = pd.concat(
            [self.pending_nodes_ingoing, path_ingoing], axis=0)
        self.pending_nodes_outgoing = pd.concat(
            [self.pending_nodes_outgoing, path_outgoing], axis=0)
        # self.info = pd.concat([self.info, info], axis=0)

        self.occurence = self._update_occurence(ingoing=path_ingoing,
                                                outgoing=path_outgoing,
                                                occurence=self.occurence)


    def _add_save_info(self):
        date_begin = datetime.now()
        date = '-'.join([str(date_begin)[:10], str(date_begin)[11:19]])

        folder_path = os.path.join(FOLDER_PATH, "data")
        if not os.path.exists(folder_path):
            os.makedirs(folder_path)

        save_folder = os.path.join(folder_path,
                                 f"{date}-{self.folder_name_suffix}")
        if os.path.exists(save_folder):
            raise ValueError("Folder to save data already exists, content will be overwritten")
        else:
            os.makedirs(save_folder)
        return save_folder

    def _update_metrics(self, iteration, found):
        self.metrics_data[iteration] = \
            self.metrics(found=found, gold_standard=self.event_gs,
                         type_metrics=self.type_metrics)


    def __call__(self):
        save_folder = self._add_save_info()
        json.dump(self.config, open(f"{save_folder}/config.json", "w", encoding='utf-8'),
                      indent=4)
        self.expanded = {}
        self.metrics_data = {}
        self.info = {}

        for i in range(1, self.iterations+1):
            print(f"Iteration {i} started at {datetime.now()}")
            output = self._run_one_iteration(iteration=i)
            self.info = self._merge_outputs(output=output, iteration=i, info=self.info)

            if self.to_expand:
                self.expanded[i+1] = self.to_expand

                self.subgraph.to_csv(f"{save_folder}/{i}-subgraph.csv")
                events_found = \
                    [str(e) for e in self.subgraph[self.subgraph.type_df == "ingoing"] \
                        .subject.unique()] + \
                        [str(e) for e in self.subgraph[self.subgraph.type_df == "outgoing"] \
                            .object.unique()]

                self._update_metrics(iteration=i, found=events_found)
                self.pending_nodes_ingoing.to_csv(f"{save_folder}/{i}-pending_nodes_ingoing.csv")
                self.pending_nodes_outgoing.to_csv(f"{save_folder}/{i}-pending_nodes_outgoing.csv")
                json.dump(self.occurence, open(f"{save_folder}/{i}-occurences.json",
                                                "w", encoding='utf-8'),
                        indent=4)
                # self.info.to_csv(f"{i}-info.csv")
                json.dump(self.expanded, open(\
                    f"{save_folder}/expanded.json", "w", encoding='utf-8'),
                        indent=4)
                json.dump(self.metrics_data, open(\
                    f"{save_folder}/metrics.json", "w", encoding='utf-8'),
                        indent=4)
                json.dump(self.info, open(\
                    f"{save_folder}/info.json", "w", encoding='utf-8'),
                        indent=4)

                print(f"Iteration {i} finished at {datetime.now()}\n=====")

                self.plotter(info=json.load(open(f"{save_folder}/metrics.json",
                                         "r", encoding="utf-8")),
                     save_folder=save_folder)

            else:
                print("According to params, no further nodes to expand," \
                    + f"finishing process at {datetime.now()}\n=====")
                break


if __name__ == '__main__':
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("-j", "--json", required=True,
                    help="Path to json file containing configuration file")
    json_path = vars(ap.parse_args())["json"]

    config_loaded = json.load(open(json_path, "r", encoding="utf-8"))
    config_loaded["rdf_type"] = [(name, URIRef(link)) \
        for name, link in config_loaded["rdf_type"].items()]

    framework = GraphSearchFramework(config=config_loaded)
    start = datetime.now()
    print(f"Process started at {start}")
    framework()
    end = datetime.now()
    print(f"Process ended at {end}, took {end-start}")
    print(framework.ordering.superclasses)
    print(framework.ordering.domain)
    print(framework.ordering.range)
