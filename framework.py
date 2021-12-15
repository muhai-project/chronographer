import pandas as pd
from time import sleep
from rdflib.term import URIRef
import multiprocessing as mp
from datetime import datetime
from expansion import NodeExpansion
from collections import defaultdict
from ranker import Ranker

config = {
    # "rdf_type": [("event", URIRef("http://dbpedia.org/ontology/Event")),
    #              ("person", URIRef("http://dbpedia.org/ontology/Person"))],
    "rdf_type": [("event", URIRef("http://dbpedia.org/ontology/Event")),],
    "predicate_filter": [],
    "start": "http://dbpedia.org/resource/Category:French_Revolution",
    "iterations": 2,
    "type_ranking": "inverse_pred_object_freq",
    "type_interface": "sparql",
}

class GraphSearchFramework:

    def __init__(self, config: dict):
        """
        Type of ranking strategies implemented:
            - pred_freq:
            - inverse_pred_freq: 
            - pred_object_freq: 
            - inverse_pred_object_freq: 
            - subject_freq: 
            - inverse_subject_freq: 
            - inverse_pred_object_split_freq: 
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
                                     "pred_object_freq", "inverse_pred_object_freq",
                                     "subject_freq", "inverse_subject_freq", "inverse_pred_object_split_freq"]:
            raise ValueError("Type ranking not implemented")
        self.type_interface = config["type_interface"]

        self._init()

        self.nb_cpu = mp.cpu_count()
        self.paths = list()

        self.ranker = Ranker(type_ranking=self.type_ranking)
        self.nodes_expanded = list()
        self.occurences = defaultdict(int)
        self.to_expand = None
    
    def _check_config(self, config: dict):
        # TO DO init: check keys + correct format for values
        # TO DO init: more readable format for prefixes?
        return 
    
    def _init(self):
        self.subgraph = pd.DataFrame(columns=["subject", "predicate", "object"])
        self.pending_nodes = pd.DataFrame(columns=["subject", "predicate", "object"])
        self.info = pd.DataFrame(columns=["path", "iteration", "tot"] + [x for elt in self.rdf_type for x in [f"{elt[0]}"] ])
    
    def _select_nodes_to_expand(self):
        if self.to_expand:
            # TO DO init: check if still pending nodes/info, and end process if needed (edge cases)
            path = [self.to_expand[0]]

            # Gettings args for next iteration
            if type(self.to_expand[0]) is str:
                pred, obj = self.to_expand[0].split(";")
                pred, obj = URIRef(pred), URIRef(obj)
                nodes = list(self.pending_nodes[(self.pending_nodes.predicate == pred) & \
                            (self.pending_nodes.object == obj)].subject.values)
            else:
                nodes = list(self.pending_nodes[(self.pending_nodes.predicate == self.to_expand[0])].subject.values)
            
            print(nodes)

            # TO DO heuristics: either path based on pred+object, either based on pred score only

        else:  # INIT state: only starting node
            path, nodes = list(), [self.start]

        return nodes, path
    
    def _expand_one_node(self, args):
        node_expander = NodeExpansion(rdf_type=self.rdf_type,
                                      iteration=args["iteration"],
                                      type_interface=args["type_interface"])
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

        output = list()
        for i, args in enumerate([{"node": node,
                      "path": path,
                      "predicate": self.predicate_filter,
                      "iteration": iteration,
                      "type_interface": self.type_interface
                      } for node in nodes_to_expand]):
            print(f"Processing node {i+1}/{len(nodes_to_expand)}")
            self.nodes_expanded.append(args["node"])
            output.append(self._expand_one_node(args))
            sleep(0.2)

        return output
    
    def _update_occurence(self, df):
        if self.type_ranking in ["pred_freq", "inverse_pred_freq"]:  # predicate
            for _, row in df.iterrows():
                self.occurences[row.predicate] += 1
        if self.type_ranking in ["pred_object_freq", "inverse_pred_object_freq"]:  # subject predicate
            for _, row in df.iterrows():
                self.occurences[f"{str(row.predicate)};{str(row.object)}"] += 1
        if self.type_ranking in ["subject_freq", "inverse_subject_freq", "inverse_pred_object_split_freq"]:
            for _, row in df.iterrows():
                self.occurences[row.subject] += 1

    
    def _merge_outputs(self, output):
        for subgraph, pending, info in output:
            # TO DO heuristics: updating ranking after each iteration (with global and not just local)
            self.subgraph = pd.concat([self.subgraph, subgraph], axis=0)
            self.pending_nodes = pd.concat([self.pending_nodes, pending], axis=0)
            self.info = pd.concat([self.info, info], axis=0)

            self._update_occurence(df=pending)
        
        self.to_expand = self.ranker(occurences=self.occurences)
        self.pending_nodes = self.pending_nodes[~self.pending_nodes.object.isin(self.nodes_expanded)]
        return 
    
    def __call__(self):
        for i in range(1, self.iterations+1):
            print(f"Iteration {i} started at {datetime.now()}")
            output = self._run_one_iteration(iteration=i)
            self._merge_outputs(output=output)

            self.subgraph.to_csv(f"{i}-subgraph.csv")
            self.pending_nodes.to_csv(f"{i}-pending_nodes.csv")
            self.info.to_csv(f"{i}-info.csv")

            print(f"Iteration {i} finished at {datetime.now()}\n=====")

        return


if __name__ == '__main__':
    framework = GraphSearchFramework(config=config)
    start = datetime.now()
    print(f"Process started at {start}")
    framework()
    end = datetime.now()
    print(f"Process ended at {end}, took {end-start}")