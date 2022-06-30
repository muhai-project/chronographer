"""
Expanding one node by retrieving its ingoing and outgoing edges
Filtering subgraph and pending nodes to be explored
"""
from copy import deepcopy
from collections import defaultdict
from src.filtering import Filtering

class NodeExpansion:
    """
    #TO DO: add documentation on this script
    interface: either a src.sparql_interface.SPARQLInterface class,
    or a src.triply_interface.TriplInterface,
    or a src.hdt_interface.HDTInterface
    """

    def __init__(self, rdf_type: list[tuple], args_filtering: dict,
                 interface):

        self.interface = interface
        self.rdf_type = rdf_type
        self.stop_classes = [elt[1] for elt in rdf_type]
        self._check_args()

        self.mapping = {uri: name for (name, uri) in rdf_type}

        self.filtering = Filtering(args=args_filtering)
        self.superclasses = defaultdict(list)

    def _check_args(self):
        if not any(elt in repr(self.interface) for elt in \
            ["src.sparql_interface.SPARQLInterface",
             "src.triply_interface.TriplInterface",
             "src.hdt_interface.HDTInterface",
             'MagicMock']):
            raise ValueError('Wrong type of `interface` passed as arguments')

        if (not isinstance(self.rdf_type, list)) or not self.rdf_type:
            raise ValueError('`rdf_type` param should be a non-empty list of tuples')
        if any(not (isinstance(elt, tuple) and len(elt) == 2) for elt in self.rdf_type):
            raise ValueError('`rdf_type` param should be a list of tuples')
        if any(not ((isinstance(a, str)) and isinstance(b, str)) \
            for (a, b) in self.rdf_type):
            raise ValueError("Type of two-element tuples should be" \
                    + "(str, str)")

    def get_output_triples(self, node, predicate):
        """ Direct call to _get_output_triples """
        return self._get_output_triples(node, predicate)

    def _get_output_triples(self, node, predicate):
        return self.interface(node=node, predicate=predicate)

    def search_superclass(self, node: str):
        """ Returns superclasses of a class """
        triples = self.interface.run_request(
            params=dict(subject=str(node)),
            filter_pred=self.interface.dataset_config["sub_class_of"],
            filter_keep=True
        )
        return [elt[2] for elt in triples]

    def superclass_search(self, superclasses: dict,
                          nodes: str, stop_class: list):
        """ Searches superclasses (class+n) until stop criterion """
        pending = [elt for elt in nodes if elt not in superclasses]
        while pending:
            node = pending[0]
            pending = pending[1:]
            curr_sup = self.search_superclass(node)
            superclasses[node] = curr_sup

            cand = [x for x in curr_sup if x not in stop_class]
            cand = [x for x in curr_sup if x not in superclasses]
            pending += cand
        return superclasses

    @staticmethod
    def rearrange_superclasses(superclasses):
        """ Map node to oldest ancestor in terms of subclass """
        output = deepcopy(superclasses)
        for k, sup_cl in superclasses.items():
            for node in [x for x in sup_cl if x in superclasses]:
                output[k] += deepcopy(superclasses[node])
        return {k: list(set(v)) for k, v in output.items()}

    def filter_sub_graph(self, type_date_df, triple_ingoing, triple_outgoing, dates):
        """ Direct call to _filter_sub_graph """
        return self._filter_sub_graph(type_date_df, triple_ingoing, triple_outgoing, dates)

    def update_superclasses(self, nodes):
        """ Update superclasses with new expanded nodes """
        self.superclasses = self.superclass_search(
            superclasses=deepcopy(self.superclasses), nodes=nodes,
            stop_class=self.stop_classes)
        self.superclasses = self.rearrange_superclasses(deepcopy(self.superclasses))

    def _filter_sub_graph(self, type_date_df, triple_ingoing, triple_outgoing, dates):
        """ Filtering subgraph: nodes to be removed, nodes to be kept, other """
        
        # Edge case: type_date_df is empty
        # --> we assume that the ingoing/outgoing nodes are not relevant for the search
        if type_date_df.shape[0] == 0:
            to_keep = []
            to_discard = list(triple_ingoing.subject.unique()) + \
                list(triple_outgoing.object.unique())

        else:
            to_discard = self.filtering(ingoing=triple_ingoing, outgoing=triple_outgoing,
                                        type_date=type_date_df, dates=dates)
            # print(to_discard)
            nodes = [elt for elt in \
                type_date_df[type_date_df.predicate == self.interface.dataset_config["rdf_type"]].object.unique() \
                if str(elt).startswith(self.interface.dataset_config["start_uri"])]
            self.update_superclasses(nodes=nodes)
            filtered = [k for k, sup_class in self.superclasses.items() \
                if any(elt in sup_class for elt in self.mapping.keys())] + \
                    list(self.mapping.keys())
            # Filter on types of nodes that should be retrieved
            # to_keep = list(type_date_df[(~type_date_df.subject.isin(to_discard)) & \
            #     (type_date_df.object.isin(list(self.mapping.keys())))].subject.unique())
            to_keep = list(type_date_df[(~type_date_df.subject.isin(to_discard)) & \
                (type_date_df.object.isin(filtered))].subject.unique())

        # print(to_keep)

        return triple_ingoing[triple_ingoing.subject.isin(to_keep)], \
            triple_ingoing[~triple_ingoing.subject.isin(to_discard)], \
            triple_outgoing[triple_outgoing.object.isin(to_keep)], \
            triple_outgoing[~triple_outgoing.object.isin(to_discard)], \
            to_discard



    def __call__(self, args, dates):

        # Updating path
        # new_path = args["path"] + [args["node"]]

        # Querying knowledge base
        ingoing, outgoing, types_date = self._get_output_triples(
            node=args["node"], predicate=args["predicate"])
        # type_df_modified = pd.merge(type_df[["subject", "object"]],
        #                             path_df[["subject", 'predicate']],
        #                             how="left", on="subject")[["subject", "predicate", "object"]]

        # Interesting new paths
        # info = self._get_info_from_type(type_df=type_df_modified, path=new_path)

        # Filter subgraph to keep
        return self._filter_sub_graph(type_date_df=types_date, triple_ingoing=ingoing,
                                      triple_outgoing=outgoing, dates=dates)




if __name__ == '__main__':
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("-i", "--interface", required=True,
                    help="Type of interface to use, either `triply` or `hdt`")
    interface_type = vars(ap.parse_args())["interface"]

    if interface_type == 'triply':
        from src.triply_interface import TriplInterface
        interface_main = TriplInterface()

    elif interface_type == 'hdt':
        from src.hdt_interface import HDTInterface
        interface_main = HDTInterface()

    else:
        raise ValueError('-i parameter should be either `triply` or `hdt`')


    NODE = "http://dbpedia.org/resource/Antoine_Morlot"
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
    RDF_TYPE = [("event", "http://dbpedia.org/ontology/Event")]

    node_expander = NodeExpansion(interface=interface_main,
                                  rdf_type=RDF_TYPE, args_filtering={"where": 1, "when": 1})
    subgraph_ingoing_test, path_ingoing_test, subgraph_outgoing_test, \
        path_outgoing_test, to_discard_test = \
            node_expander(args={"path": [], "node": NODE, "predicate": PREDICATE},
                          dates=["1789-01-01", "1799-12-31"])
    print(f"{subgraph_ingoing_test}\n{path_ingoing_test}")
    print(f"{subgraph_outgoing_test}\n{path_outgoing_test}")
    print(f"\nTO DISCARD\n{to_discard_test}\n")
