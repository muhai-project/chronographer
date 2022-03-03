"""
Expanding one node by retrieving its ingoing and outgoing edges
Filtering subgraph and pending nodes to be explored
"""
from rdflib.term import URIRef
from src.filtering import Filtering
from src.triply_interface import TriplInterface

class NodeExpansion:
    """
    #TO DO: add documentation on this script
    interface: either a src.sparql_interface.SPARQLInterface class,
    or a src.triply_interface.TriplInterface
    """

    def __init__(self, rdf_type: list[tuple], args_filtering: dict,
                 interface=TriplInterface()):

        self.interface = interface
        self.rdf_type = rdf_type
        self._check_args()

        self.type_pred = URIRef("http://www.w3.org/1999/02/22-rdf-syntax-ns#type")
        self.mapping = {uri: name for (name, uri) in rdf_type}

        self.filtering = Filtering(args=args_filtering)

    def _check_args(self):
        print(repr(self.interface))
        if not any(elt in repr(self.interface) for elt in \
            ["src.sparql_interface.SPARQLInterface",
             "src.triply_interface.TriplInterface",
             "src.hdt_interface.HDTInterface",
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

    def filter_sub_graph(self, type_date_df, triple_ingoing, triple_outgoing, dates):
        """ Direct call to _filter_sub_graph """
        return self._filter_sub_graph(type_date_df, triple_ingoing, triple_outgoing, dates)

    def _filter_sub_graph(self, type_date_df, triple_ingoing, triple_outgoing, dates):
        # Getting nodes to discard (both for subgraph and pending nodes)

        # to_keep_date = date_df[(date_df.object >= dates[0]) & \
        #                        (date_df.object <= dates[1])].subject.unique()
        to_discard = self.filtering(df_pd=type_date_df, dates=dates)
        print(to_discard)

        # Filter on types of nodes that should be retrieved
        to_keep = list(type_date_df[(~type_date_df.subject.isin(to_discard)) & \
            (type_date_df.object.isin(list(self.mapping.keys())))].subject.unique())

        print(to_keep)

        return triple_ingoing[triple_ingoing.subject.isin(to_keep)], \
            triple_ingoing[~triple_ingoing.subject.isin(to_discard)], \
            triple_outgoing[triple_outgoing.object.isin(to_keep)], \
            triple_outgoing[~triple_outgoing.object.isin(to_discard)], \
            to_discard



    def __call__(self, args, dates):

        # Updating path
        # new_path = args["path"] + [args["node"]]

        # Querying knowledge base
        ingoing, outgoing, types_and_date = self._get_output_triples(
            node=args["node"], predicate=args["predicate"])
        # type_df_modified = pd.merge(type_df[["subject", "object"]],
        #                             path_df[["subject", 'predicate']],
        #                             how="left", on="subject")[["subject", "predicate", "object"]]

        # Interesting new paths
        # info = self._get_info_from_type(type_df=type_df_modified, path=new_path)

        # Filter subgraph to keep
        return self._filter_sub_graph(type_date_df=types_and_date, triple_ingoing=ingoing,
                                      triple_outgoing=outgoing, dates=dates)




if __name__ == '__main__':
    NODE = "http://dbpedia.org/resource/Rhode_Island"
    PREDICATE = ["http://dbpedia.org/ontology/wikiPageWikiLink",
                    "http://dbpedia.org/ontology/wikiPageRedirects",
                    "http://dbpedia.org/ontology/wikiPageDisambiguates",
                    "http://www.w3.org/2000/01/rdf-schema#seeAlso",
                    "http://xmlns.com/foaf/0.1/depiction",
                    "http://xmlns.com/foaf/0.1/isPrimaryTopicOf",
                    "http://dbpedia.org/ontology/thumbnail",
                    "http://dbpedia.org/ontology/wikiPageExternalLink",
                    "http://dbpedia.org/ontology/wikiPageID",
                    "http://dbpedia.org/ontology/wikiPageLength",
                    "http://dbpedia.org/ontology/wikiPageRevisionID",
                    "http://dbpedia.org/property/wikiPageUsesTemplate",
                    "http://www.w3.org/2002/07/owl#sameAs",
                    "http://www.w3.org/ns/prov#wasDerivedFrom"]
    RDF_TYPE = [("event", URIRef("http://dbpedia.org/ontology/Event"))]

    node_expander = NodeExpansion(rdf_type=RDF_TYPE, args_filtering={"who": 0, "when": 0})
    subgraph_ingoing_test, path_ingoing_test, subgraph_outgoing_test, \
        path_outgoing_test, to_discard_test = \
            node_expander(args={"path": [], "node": NODE, "predicate": PREDICATE},
                        dates=["1789-01-01", "1799-12-31"])
    print(f"{subgraph_ingoing_test}\n{path_ingoing_test}")
    print(f"{subgraph_outgoing_test}\n{path_outgoing_test}")
    print(f"\nTO DISCARD\n{to_discard_test}\n")
