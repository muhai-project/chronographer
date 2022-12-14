"""
Converting graph to format suitable for rule miner system (eg AnyBURL or TLogic)
"""
from hdt import HDTDocument
from rdflib import Graph

def get_triples(graph, type_graph):
    """ Return all triples in graph """
    if type_graph == 'ttl':
        return graph.triples((None, None, None))
    if type_graph == "hdt":
        return graph.search_triples("", "", "")[0]
    raise ValueError(f"{type_graph} not a valid type_graph")

def convert_hdt(graph, type_graph, output_file):
    """ Converting graph into suitable .txt format
    Assuming size of graph doc not too big for scaling 
    graph can be of type: `ttl` or `hdt` """
    triples = get_triples(graph, type_graph)
    f_res = open(output_file, "w+", encoding="utf-8")

    for sub, pred, obj in triples:
        f_res.write(f"{sub}\t{pred}\t{obj}\n")
    f_res.close()

PATH = "wikidata-causal-2022-10-13/wikidatacausal20221013.hdt"
GRAPH = HDTDocument(PATH)
OUTPUT_FILE = "test_hdt.txt"
convert_hdt(GRAPH, "hdt", OUTPUT_FILE)

PATH = "wikidata-causal-2022-10-13/wikidata_cc_full_3_hop.ttl"
GRAPH = Graph()
GRAPH.parse(PATH)
OUTPUT_FILE = "test_ttl.txt"
convert_hdt(GRAPH, "ttl", OUTPUT_FILE)
