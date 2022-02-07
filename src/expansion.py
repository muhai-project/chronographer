"""
Expanding one node by retrieving its ingoing and outgoing edges
Filtering subgraph and pending nodes to be explored
"""
from rdflib.term import URIRef
from src.triply_interface import TriplInterface

class NodeExpansion:
    """
    #TO DO: add documentation on this script
    interface: either a src.sparql_interface.SPARQLInterface class,
    or a src.triply_interface.TriplInterface
    """

    def __init__(self, rdf_type: list[tuple], iteration: int,
                 interface=TriplInterface(
                     default_pred=["http://www.w3.org/1999/02/22-rdf-syntax-ns#type"])):

        self.interface = interface
        self.rdf_type = rdf_type
        self.iter = iteration
        self._check_args()

        self.type_pred = URIRef("http://www.w3.org/1999/02/22-rdf-syntax-ns#type")
        self.mapping = {uri: name for (name, uri) in rdf_type}

    def _check_args(self):
        if not any(elt in repr(self.interface) for elt in \
            ["src.sparql_interface.SPARQLInterface",
             "src.triply_interface.TriplInterface",
             'MagicMock']):
            raise ValueError('Wrong type of `interface` passed as arguments')

        if (not isinstance(self.rdf_type, list)) or not self.rdf_type:
            raise ValueError('`rdf_type` param should be a non-empty list of tuples')
        else:
            if any(not (isinstance(elt, tuple) and len(elt) == 2) for elt in self.rdf_type):
                raise ValueError('`rdf_type` param should be a list of tuples')
            else:
                if any(not ((isinstance(a, str)) and isinstance(b, URIRef)) \
                    for (a, b) in self.rdf_type):
                    raise ValueError("Type of two-element tuples should be" \
                            + "(str, type('rdflib.term.URIRef'))")

        if not isinstance(self.iter, int):
            raise ValueError("`iteration` param should be an integer")

    def get_output_triples(self, node, predicate):
        """ Direct call to _get_output_triples """
        return self._get_output_triples(node, predicate)

    def _get_output_triples(self, node, predicate):
        return self.interface(node=node, predicate=predicate)

    # def _get_info_from_type(self, type_df, path):

    #     type_df["type"] = type_df["object"].apply( \
    #         lambda x: self.mapping[x] if x in self.mapping else "other")

    #     # grouped = type_df.groupby(["subject", "predicate", "type"]).agg({"object": "count"})
    #     grouped = type_df.groupby(["predicate", "type"]).agg({"object": "count"})
    #     # for _ in range(3):
    #     for _ in range(2):
    #         grouped.reset_index(level=0, inplace=True)
    #     grouped["path"] =  [','.join([elt for elt in path] + \
    #         [str(grouped.predicate.values[i])]) for i in range(grouped.shape[0])]
    #     info = grouped.pivot(index="path", columns="type", values="object").fillna(0).astype(int)
    #     columns = info.columns
        # info["tot"] = [sum([info[col].values[i] for col in columns]) \
        #     for i in range(info.shape[0])]
    #     info["iteration"] = self.iter

    #     return info

    def filter_sub_graph(self, type_df, triple_ingoing, triple_outgoing):
        """ Direct call to _filter_sub_graph """
        return self._filter_sub_graph(type_df, triple_ingoing, triple_outgoing)

    def _filter_sub_graph(self, type_df, triple_ingoing, triple_outgoing):
        to_keep = type_df[type_df.object.isin(list(self.mapping.keys()))].subject.values
        return triple_ingoing[triple_ingoing.subject.isin(to_keep)], \
            triple_ingoing[~triple_ingoing.subject.isin(to_keep)], \
            triple_outgoing[triple_outgoing.object.isin(to_keep)], \
            triple_outgoing[~triple_outgoing.object.isin(to_keep)]



    def __call__(self, args):
        # TO DO heuristics: check if needs update

        # Updating path
        # new_path = args["path"] + [args["node"]]

        # Querying knowledge base
        ingoing, outgoing, types = self._get_output_triples(
            node=args["node"], predicate=args["predicate"])
        # type_df_modified = pd.merge(type_df[["subject", "object"]],
        #                             path_df[["subject", 'predicate']],
        #                             how="left", on="subject")[["subject", "predicate", "object"]]

        # Interesting new paths
        # info = self._get_info_from_type(type_df=type_df_modified, path=new_path)

        # Filter subgraph to keep
        subgraph_ingoing, path_ingoing, subgraph_outgoing, path_outgoing = \
            self._filter_sub_graph(type_df=types, triple_ingoing=ingoing,
                                   triple_outgoing=outgoing)

        return subgraph_ingoing, path_ingoing, subgraph_outgoing, path_outgoing



if __name__ == '__main__':
    NODE = "http://dbpedia.org/resource/Category:French_Revolution"
    PREDICATE = []
    RDF_TYPE = [("event", URIRef("http://dbpedia.org/ontology/Event")),
                ("person", URIRef("http://dbpedia.org/ontology/Person"))]
    ITERATION = 2

    node_expander = NodeExpansion(rdf_type=RDF_TYPE, iteration=ITERATION)
    subgraph_ingoing_test, path_ingoing_test, subgraph_outgoing_test, path_outgoing_test = \
        node_expander(args={"path": [], "node": NODE, "predicate": PREDICATE})
    print(f"{subgraph_ingoing_test}\n{path_ingoing_test}")
    print(f"{subgraph_outgoing_test}\n{path_outgoing_test}")
