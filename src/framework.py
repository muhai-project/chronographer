"""
#TO DO: add documentation on this script
"""
from time import sleep
import multiprocessing as mp
from datetime import datetime
from collections import defaultdict

import pandas as pd
from rdflib.term import URIRef
from src.ranker import Ranker
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
    "iterations": 10,
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
            - inverse_pred_freq:
            - pred_object_freq:
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
        self.interface = TriplInterface() if config["type_interface"] == 'triply' \
            else SPARQLInterface()

        self.type_interface = config["type_interface"]

        self.subgraph = pd.DataFrame(columns=["subject", "predicate", "object"])
        self.pending_nodes = pd.DataFrame(columns=["subject", "predicate", "object"])
        self.info = pd.DataFrame(columns=["path", "iteration", "tot"] + \
            [x for elt in self.rdf_type for x in [f"{elt[0]}"] ])

        self.nb_cpu = mp.cpu_count()
        self.paths = list()

        self.ranker = Ranker(type_ranking=self.type_ranking)
        self.nodes_expanded = list()
        self.occurence = defaultdict(int)
        self.to_expand = None

    def _check_config(self, config: dict):
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
                nodes = list(self.pending_nodes[(self.pending_nodes.predicate == pred) & \
                            (self.pending_nodes.object == obj)].subject.values)
            else:
                nodes = list(self.pending_nodes[\
                    (self.pending_nodes.predicate == self.to_expand)].subject.values)

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
        for subgraph, pending in output:
            # TO DO heuristics: updating ranking
            # after each iteration (with global and not just local)
            self.subgraph = pd.concat([self.subgraph, subgraph], axis=0)
            self.pending_nodes = pd.concat([self.pending_nodes, pending], axis=0)
            # self.info = pd.concat([self.info, info], axis=0)

            self.occurence = self._update_occurence(dataframe=pending,
                                                    occurence=self.occurence)

        self.to_expand = self.ranker(occurences=self.occurence)
        self.occurence = defaultdict(int, {k:v for k, v in self.occurence.items() if k != self.to_expand})
        self.expanded.append(self.to_expand)
        self.pending_nodes = self.pending_nodes[\
            ~self.pending_nodes.subject.isin(self.nodes_expanded)]
        


    def __call__(self):
        self.expanded = []
        for i in range(1, self.iterations+1):
            print(f"Iteration {i} started at {datetime.now()}")
            output = self._run_one_iteration(iteration=i)
            self._merge_outputs(output=output)

            self.subgraph.to_csv(f"{i}-subgraph.csv")
            self.pending_nodes.to_csv(f"{i}-pending_nodes.csv")
            import json
            json.dump(self.occurence, open(f"{i}-occurences.csv", "w"), indent=4)
            # self.info.to_csv(f"{i}-info.csv")

            print(f"Iteration {i} finished at {datetime.now()}\n=====")
        
        for elt in self.expanded:
            print(elt)



if __name__ == '__main__':
    framework = GraphSearchFramework(config=CONFIG)
    start = datetime.now()
    print(f"Process started at {start}")
    framework()
    end = datetime.now()
    print(f"Process ended at {end}, took {end-start}")
