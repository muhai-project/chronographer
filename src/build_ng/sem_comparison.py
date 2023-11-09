# -*- coding: utf-8 -*-
"""
Comparing two graphs
"""
from collections import defaultdict
from src.helpers.variables import NS_SEM, STR_SEM, PREFIX_SEM
from src.helpers.graph_structure import get_intersection_difference

class SEMComparer:
    """ Comparing wrt. SEM predicates """
    def __init__(self):
        self.predicates = {
            "place": NS_SEM["hasPlace"],
            "actor": NS_SEM["hasActor"],
            "begin_ts": NS_SEM["hasBeginTimeStamp"],
            "end_ts": NS_SEM["hasEndTimeStamp"],
            "ts": NS_SEM["hasTimeStamp"],
            "sub_event": NS_SEM["subEventOf"],
        }
        self.pred_to_prefix = {
            STR_SEM: PREFIX_SEM
        }

    def count_pred(self, graph):
        """ Count number of unique preds in the graph
        NB: to be replaced with a SPARQL query """
        res = defaultdict(int)
        for triple in graph:
            key = triple[1]
            for k, v in self.pred_to_prefix.items():
                key = key.replace(k, f"{v}:")
            res[key] += 1
        return res
    

    def __call__(self, graph_c, graph_gs):
        print(f"# of triples in graph_c: {len(graph_c)}")
        print(f"# of triples in graph_gs: {len(graph_gs)}")

        intersection, graph_c_only, graph_gs_only = get_intersection_difference(g1=graph_c, g2=graph_gs)

        print(f"# of triples in both: {len(intersection)}")
        print(f"# of triples in graph_c only: {len(graph_c_only)}")
        print(f"# of triples in graph_gs only: {len(graph_gs_only)}")

        pred_i, pred_c, pred_gs = self.count_pred(intersection), self.count_pred(graph_c_only), self.count_pred(graph_gs_only)

        print(pred_i)
        print("=====")
        print(pred_c)
        print("=====")
        print(pred_gs)
        print("=====")
        # print(f"Intersection\\n{'\\n'.join([pred + ': ' + count for pred, count in pred_i.items()])}\\n=====")



if __name__ == '__main__':
    from rdflib import Graph

    GRAPH_C = Graph()
    GRAPH_C.parse("kg_transformation/kg.ttl", format="ttl")

    GRAPH_GS = Graph()
    GRAPH_GS.parse("kg_transformation/eventkg_french_rev.ttl", format="ttl")

    COMPARER = SEMComparer()
    COMPARER(graph_c=GRAPH_C, graph_gs=GRAPH_GS)
