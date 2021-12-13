import pandas as pd
from time import sleep
from rdflib.term import URIRef
import multiprocessing as mp
from datetime import datetime
from expansion import NodeExpansion

config = {
    # "rdf_type": [("event", URIRef("http://dbpedia.org/ontology/Event")),
    #              ("person", URIRef("http://dbpedia.org/ontology/Person"))],
    "rdf_type": [("event", URIRef("http://dbpedia.org/ontology/Event")),],
    "predicate_filter": [],
    "start": "http://dbpedia.org/resource/Category:French_Revolution",
    "iterations": 5,
    "type_ranking": "frequency_predicate",
    "type_interface": "sparql",
}

class GraphSearchFramework:

    def __init__(self, config: dict):
        # TO DO heuristics: change saving format (proper folder with timestamp etc etc)
        # TO DO heuristics: add searcg strategy (update type_ranking)
        self._check_config(config=config)
        self.config = config
        self.rdf_type = config["rdf_type"]
        self.predicate_filter = config["predicate_filter"]
        self.start = config["start"]
        self.iterations = config["iterations"]
        self.type_ranking = config["type_ranking"]
        self.type_interface = config["type_interface"]

        self._init()

        self.nb_cpu = mp.cpu_count()
        self.paths = list()
    
    def _check_config(self, config: dict):
        # TO DO init: check keys + correct format for values
        # TO DO init: more readable format for prefixes?
        return 
    
    def _init(self):
        self.ranked_paths = list()
        self.subgraph = pd.DataFrame(columns=["subject", "predicate", "object"])
        self.pending_nodes = pd.DataFrame(columns=["subject", "predicate", "object"])
        self.info = pd.DataFrame(columns=["path", "iteration", "tot"] + [x for elt in self.rdf_type for x in [f"{elt[0]}"] ])
    
    def _select_nodes_to_expand(self):
        if self.ranked_paths:
            # TO DO init: check if still pending nodes/info, and end process if needed (edge cases)

            # Gettings args for next iteration
            path = self.ranked_paths[0][0]
            self.paths.append(path)

            # TO DO heuristics: either path based on pred+object, either based on pred score only
            predicate, object_entity = path[-1], path[-2]
            nodes = list(self.pending_nodes[(self.pending_nodes.predicate == URIRef(predicate)) & \
                                            (self.pending_nodes.object == URIRef(object_entity))].subject.values)
            
            # Updating parameters
            self.ranked_paths = self.ranked_paths[1:]
            self.pending_nodes = self.pending_nodes[~self.pending_nodes.subject.isin(nodes)]

        else:  # INIT state: only starting node
            path, nodes = list(), [self.start]

        return nodes, path
    
    def _expand_one_node(self, args):
        node_expander = NodeExpansion(rdf_type=self.rdf_type,
                                      iteration=args["iteration"],
                                      type_ranking=args["type_ranking"],
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
                      "type_ranking": self.type_ranking,
                      "type_interface": self.type_interface
                      } for node in nodes_to_expand]):
            print(f"Processing node {i+1}/{len(nodes_to_expand)}")
            output.append(self._expand_one_node(args))
            sleep(0.2)

        return output

    
    def _merge_outputs(self, output):
        for ranked_paths, subgraph, pending, info in output:
            # TO DO heuristics: updating ranking after each iteration (with global and not just local)
            self.ranked_paths += ranked_paths
            self.subgraph = pd.concat([self.subgraph, subgraph], axis=0)
            self.pending_nodes = pd.concat([self.pending_nodes, pending], axis=0)
            self.info = pd.concat([self.info, info], axis=0)
        
        self.ranked_paths = sorted(self.ranked_paths, key=lambda x: x[1], reverse=True)
        return 
    
    def __call__(self):
        for i in range(1, self.iterations+1):
            print(f"Iteration {i} started at {datetime.now()}")
            output = self._run_one_iteration(iteration=i)
            self._merge_outputs(output=output)

            self.subgraph.to_csv(f"{i}-subgraph.csv")
            self.pending_nodes.to_csv(f"{i}-pending_nodes.csv")
            self.info.to_csv(f"{i}-info.csv")

            # print(self.ranked_paths)
            print(f"Iteration {i} finished at {datetime.now()}\n=====")
        
        print(self.paths)
        f = open("paths.txt", "w+")
        for elt in self.paths:
            f.write(f"{','.join([str(x) for x in elt])}\n")
        f.close()

        return


if __name__ == '__main__':
    framework = GraphSearchFramework(config=config)
    start = datetime.now()
    print(f"Process started at {start}")
    framework()
    end = datetime.now()
    print(f"Process ended at {end}, took {end-start}")