"""
#TO DO: add documentation on this script
"""
import os
import json
from time import sleep
import multiprocessing as mp
from datetime import datetime
from collections import defaultdict

import pandas as pd
from rdflib.term import URIRef
from settings import FOLDER_PATH
from src.ranker import Ranker
from src.metrics import Metrics
from src.plotter import Plotter
from src.expansion import NodeExpansion
from src.triply_interface import TriplInterface
from src.sparql_interface import SPARQLInterface


CONFIG = {
    # "rdf_type": [("event", URIRef("http://dbpedia.org/ontology/Event")),
    #              ("person", URIRef("http://dbpedia.org/ontology/Person"))],
    "rdf_type": [("event", URIRef("http://dbpedia.org/ontology/Event")),],
    "predicate_filter": ["http://dbpedia.org/ontology/wikiPageWikiLink",
                         "http://dbpedia.org/ontology/wikiPageRedirects"],
    "start": "http://dbpedia.org/resource/Category:French_Revolution",
    "iterations": 2,
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
        # TO DO heuristics: change saving format (proper folder with timestamp etc etc)
        # TO DO heuristics: add searcg strategy (update type_ranking)
        self._check_config(config=config)
        self.config = config
        self.rdf_type = config["rdf_type"]
        self.predicate_filter = config["predicate_filter"]
        self.start = config["start"]
        self.iterations = config["iterations"]
        self.type_ranking = config["type_ranking"]
        if self.type_ranking not in ["pred_freq", "inverse_pred_freq",
                                     "entropy_pred_freq",
                                     "pred_object_freq", "inverse_pred_object_freq",
                                     "entropy_pred_object_freq"]:
            raise ValueError("Type ranking not implemented")

        if config["type_interface"] not in ["triply", "sparql"]:
            raise ValueError("Type of database interface not implemented")
        self.interface = TriplInterface(
            default_pred=["http://www.w3.org/1999/02/22-rdf-syntax-ns#type"]) \
            if config["type_interface"] == 'triply' \
            else SPARQLInterface()

        self.type_interface = config["type_interface"]

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

    @staticmethod
    def _check_config(config: dict):
        # TO DO init: check keys + correct format for values
        # TO DO init: more readable format for prefixes?
        return config

    def select_nodes_to_expand(self):
        """ Accessible call to _select_nodes_to_expand"""
        return self._select_nodes_to_expand()

    def _select_nodes_to_expand(self):
        if self.to_expand:
            # TO DO init: check if still pending nodes/info, and end process if needed (edge cases)
            path = [self.to_expand]

            # Gettings args for next iteration
            if ";" in self.to_expand:
                pred, obj = self.to_expand.split(";")
                pred, obj = URIRef(pred), URIRef(obj)
                nodes = list(self.pending_nodes_ingoing[(self.pending_nodes_ingoing.predicate == pred) & \
                            (self.pending_nodes_ingoing.object == obj)].subject.values)
            else:
                nodes = list(self.pending_nodes_ingoing[\
                    (self.pending_nodes_ingoing.predicate == self.to_expand)].subject.values)

            # TO DO heuristics: either path based on pred+object, either based on pred score only

        else:  # INIT state: only starting node
            path, nodes = [], [self.start]

        return nodes, path

    def _expand_one_node(self, args):
        node_expander = NodeExpansion(rdf_type=self.rdf_type,
                                      iteration=args["iteration"],
                                      interface=self.interface)
        return node_expander(args=args)

    def _run_one_iteration(self, iteration):
        nodes_to_expand, path = self._select_nodes_to_expand()

        # pool = mp.Pool(self.nb_cpu)
        # output = pool.map(lambda args: self._expand_one_node(args),
        #                     [{
        #                         "node": node,
        #                         "path": path,
        #                         "predicate": self.predicate_filter,
        #                         "iteration": iteration
        #                         } for node in nodes_to_expand])
        # pool.close()
        # pool.join()

        output = []
        for i, args in enumerate([{"node": node,
                      "path": path,
                      "predicate": self.predicate_filter,
                      "iteration": iteration,
                      } for node in nodes_to_expand]):
            print(f"Processing node {i+1}/{len(nodes_to_expand)}")
            self.nodes_expanded.append(args["node"])
            output.append(self._expand_one_node(args))
            sleep(0.2)

        return output

    def update_occurence(self, dataframe, occurence):
        """ Accessible call to _update_occurence """
        return self._update_occurence(dataframe, occurence)

    def _update_occurence(self, dataframe, occurence):
        if self.type_ranking in ["pred_freq", "entropy_pred_freq",
                                 "inverse_pred_freq"]:  # predicate
            for _, row in dataframe.iterrows():
                occurence[row.predicate] += 1
        if self.type_ranking in ["pred_object_freq",
                                 "entropy_pred_object_freq",
                                 "inverse_pred_object_freq"]:  # subject predicate
            for _, row in dataframe.iterrows():
                occurence[f"{str(row.predicate)};{str(row.object)}"] += 1
        return occurence


    def _merge_outputs(self, output):
        for subgraph_ingoing, path_ingoing, subgraph_outgoing, path_outgoing in output:
            # TO DO heuristics: updating ranking
            # after each iteration (with global and not just local)
            self.subgraph = pd.concat([self.subgraph, subgraph_ingoing], axis=0)
            self.subgraph = pd.concat([self.subgraph, subgraph_outgoing], axis=0)
            self.pending_nodes_ingoing = pd.concat(
                [self.pending_nodes_ingoing, path_ingoing], axis=0)
            self.pending_nodes_outgoing = pd.concat(
                [self.pending_nodes_outgoing, path_outgoing], axis=0)
            # self.info = pd.concat([self.info, info], axis=0)

            self.occurence = self._update_occurence(dataframe=path_ingoing,
                                                    occurence=self.occurence)

        # TO ADD: include outgoing in ranking
        self.to_expand = self.ranker(occurences=self.occurence)
        if self.to_expand:
            self.occurence = defaultdict(int, {k:v for k, v in self.occurence.items() \
                if k != self.to_expand})
            self.pending_nodes_ingoing = self.pending_nodes_ingoing[\
                ~self.pending_nodes_ingoing.subject.isin(self.nodes_expanded)]

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

        for i in range(1, self.iterations+1):
            print(f"Iteration {i} started at {datetime.now()}")
            output = self._run_one_iteration(iteration=i)
            self._merge_outputs(output=output)

            if self.to_expand:
                self.expanded[i+1] = self.to_expand

                self.subgraph.to_csv(f"{save_folder}/{i}-subgraph.csv")
                events_found = list(set(\
                [str(e) for e in self.subgraph.subject.values]))
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

                print(f"Iteration {i} finished at {datetime.now()}\n=====")

            else:
                print("According to params, no further nodes to expand," \
                    + f"finishing process at {datetime.now()}\n=====")

        self.plotter(info=json.load(open(f"{save_folder}/metrics.json",
                                         "r", encoding="utf-8")),
                     save_folder=save_folder)


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
