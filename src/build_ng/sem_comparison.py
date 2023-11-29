# -*- coding: utf-8 -*-
"""
Comparing two graphs
"""
import click
from collections import defaultdict
from rdflib import Graph
from kglab.helpers.variables import NS_SEM, STR_SEM, PREFIX_SEM
from kglab.helpers.graph_structure import get_intersection_difference

def get_f1(precision: float, recall: float) -> float:
    if precision + recall:
        return 2*precision*recall/(precision + recall)
    return 0

class SEMComparer:
    """ Comparing wrt. SEM predicates """
    def __init__(self):
        self.predicates = [str(NS_SEM["hasPlace"]), str(NS_SEM["hasActor"]), 
                           str(NS_SEM["hasBeginTimeStamp"]), str(NS_SEM["hasEndTimeStamp"])]
        self.predicates = [str(x) for x in self.predicates]
        self.pred_to_prefix = {
            STR_SEM: PREFIX_SEM
        }

    def count_pred(self, graph: Graph) -> dict:
        """ Count number of unique preds in the graph
        NB: to be replaced with a SPARQL query """
        res = defaultdict(int)
        for triple in graph:
            key = triple[1]
            for k, v in self.pred_to_prefix.items():
                key = key.replace(k, f"{v}:")
            res[key] += 1
        return res
    
    def init_query(self):
        start, end = "{", "}"
        return f"""
        SELECT ?event (COUNT(?o) as ?nb_triples) WHERE {start}
            ?event ?p ?o .
        {end}
        GROUP BY ?event
        """
    
    def remove_pred(self, graph: Graph):
        return [x for x in graph if x[1] in self.predicates]
    
    def __call__(self, graph_c: Graph, graph_gs: Graph) -> dict:
        output = {"numbers": {}, "metrics": {}, "triples": {"len_c": len(graph_c), "len_gs": len(graph_gs)}}

        intersection, graph_c_only, graph_gs_only = get_intersection_difference(g1=graph_c, g2=graph_gs)
        intersection = self.remove_pred(intersection)
        graph_c_only = self.remove_pred(graph_c_only)
        graph_gs_only = self.remove_pred(graph_gs_only)

        output["numbers"]["all"] = {
            "triples_common": len(intersection),
            "triples_search_only": len(graph_c_only),
            "triples_gs_only": len(graph_gs_only)
        }

        if len(intersection) + len(graph_gs_only):
            precision = 100*len(intersection)/(len(intersection) + len(graph_gs_only))
        else:
            precision = 0
        
        if len(intersection) + len(graph_c_only):
            recall = 100*len(intersection)/(len(intersection) + len(graph_c_only))
        else:
            recall = 0
        output["metrics"] = {
            "all": {"precision": precision, "recall": recall, "f1": get_f1(precision, recall)}
        }

        pred_i, pred_c, pred_gs = self.count_pred(intersection), self.count_pred(graph_c_only), self.count_pred(graph_gs_only)

        keys = set(pred_i.keys()).intersection(set(pred_c.keys())).intersection(set(pred_gs.keys()))
        for key in keys:
            output["numbers"][key] = {
                "triples_common": pred_i.get(key, 0),
                "triples_search_only": pred_c.get(key, 0),
                "triples_gs_only": pred_gs.get(key, 0)
            }
            if pred_i.get(key, 0) + pred_gs.get(key, 0):
                precision = 100*pred_i.get(key, 0)/(pred_i.get(key, 0) + pred_gs.get(key, 0))
            else:
                precision = 0

            if pred_i.get(key, 0) + pred_c.get(key, 0):
                recall = 100*pred_i.get(key, 0)/(pred_i.get(key, 0) + pred_c.get(key, 0))
            else:
                recall = 0
            output["metrics"] = output["metrics"] | \
                {key: {"precision": precision, "recall": recall, "f1": get_f1(precision, recall)}}
        return output


@click.command()
@click.option("--build", help=".ttl path to built narrative graph")
@click.option("--gs", help=".ttl path to ground truth narrative graph")
def main(build, gs):
    graph_c = Graph()
    graph_c.parse(build, format="ttl")

    graph_gs = Graph()
    graph_gs.parse(gs, format="ttl")

    comparer = SEMComparer()
    output = comparer(graph_c=graph_c, graph_gs=graph_gs)
    # print(output)


if __name__ == '__main__':
    main()
