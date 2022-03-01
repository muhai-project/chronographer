"""
Expanding one node by retrieving its ingoing and outgoing edges
Filtering subgraph and pending nodes to be explored
"""
import re
from rdflib.term import URIRef
import pandas as pd
from src.triply_interface import TriplInterface

class NodeExpansion:
    """
    #TO DO: add documentation on this script
    interface: either a src.sparql_interface.SPARQLInterface class,
    or a src.triply_interface.TriplInterface
    """

    def __init__(self, rdf_type: list[tuple], iteration: int,
                 interface=TriplInterface()):

        self.interface = interface
        self.rdf_type = rdf_type
        self.iter = iteration
        self._check_args()

        self.type_pred = URIRef("http://www.w3.org/1999/02/22-rdf-syntax-ns#type")
        self.mapping = {uri: name for (name, uri) in rdf_type}

        self.dates = [
            URIRef("http://dbpedia.org/ontology/date")
        ]
        self.start_dates = [
            URIRef("http://dbpedia.org/ontology/startDate"),
            URIRef("http://dbpedia.org/property/birthDate")
        ]
        self.end_dates = [
            URIRef("http://dbpedia.org/ontology/endDate"),
            URIRef("http://dbpedia.org/property/deathDate")
        ]

        self.places = [
            URIRef("http://dbpedia.org/ontology/Place"),
            URIRef("http://dbpedia.org/ontology/Location")
        ]

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

    def filter_sub_graph(self, type_date_df, triple_ingoing, triple_outgoing, dates):
        """ Direct call to _filter_sub_graph """
        return self._filter_sub_graph(type_date_df, triple_ingoing, triple_outgoing, dates)

    def get_to_discard_date(self, date_df: pd.core.frame.DataFrame, dates: list[str]):
        """ Filtering on temporal dimension
        - checking date/start date/end date """

        # return list(date_df[(date_df.object < dates[0]) | \
        #                           (date_df.object > dates[1])].subject.unique())
        return list(date_df[((date_df.predicate.isin(self.end_dates)) & \
                             (date_df.object < dates[0])) | \
                            ((date_df.predicate.isin(self.start_dates)) & \
                             (date_df.object > dates[1])) | \
                            ((date_df.predicate.isin(self.dates)) & \
                             (date_df.object < dates[0])) | \
                            ((date_df.predicate.isin(self.dates)) & \
                             (date_df.object > dates[1]))].subject.unique())

    @staticmethod
    def get_to_discard_regex(df_pd: pd.core.frame.DataFrame, dates: list[str]):
        """ Filtering on string uri
        - temporal dimension: regex on the URL (and therefore name of the events,
            e.g. 1997_National_Championships > non relevant """
        pattern = "\\d{4}"
        df_pd['regex_helper'] = df_pd.subject.apply(lambda x: re.search(pattern, str(x)))
        df_pd['regex_helper'] = df_pd.apply(
            lambda x: str(re.findall(pattern, x.subject)[0]) \
                if x['regex_helper'] else dates[0], axis=1)
        return list(df_pd[(df_pd.regex_helper < dates[0][:4]) | \
                          (df_pd.regex_helper > dates[1][:4])].subject.unique())

    def get_to_discard_location(self, df_pd: pd.core.frame.DataFrame):
        """ Location filter: retrieving nodes that correspond to locations
        (would be too broad for the search, hence later discarded """
        return list(df_pd[df_pd.object.isin(self.places)].subject.unique())

    def _filter_sub_graph(self, type_date_df, triple_ingoing, triple_outgoing, dates):
        # Filter on dates
        type_date_df.to_csv("type_date.csv")
        date_df = type_date_df[type_date_df.predicate.isin(
            self.dates + self.start_dates + self.end_dates)]
        date_df.to_csv("dates.csv")
        date_df.object = date_df.object.astype(str)

        # to_keep_date = date_df[(date_df.object >= dates[0]) & \
        #                        (date_df.object <= dates[1])].subject.unique()
        to_discard = list(set(self.get_to_discard_date(date_df=date_df, dates=dates) + \
                              self.get_to_discard_regex(df_pd=type_date_df, dates=dates) + \
                              self.get_to_discard_location(df_pd=type_date_df)))
        to_discard = [URIRef(elt) for elt in to_discard]
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
    ITERATION = 2

    node_expander = NodeExpansion(rdf_type=RDF_TYPE, iteration=ITERATION)
    subgraph_ingoing_test, path_ingoing_test, subgraph_outgoing_test, \
        path_outgoing_test, to_discard_test = \
            node_expander(args={"path": [], "node": NODE, "predicate": PREDICATE},
                        dates=["1789-01-01", "1799-12-31"])
    print(f"{subgraph_ingoing_test}\n{path_ingoing_test}")
    print(f"{subgraph_outgoing_test}\n{path_outgoing_test}")
    print(f"\nTO DISCARD\n{to_discard_test}\n")
