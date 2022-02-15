"""
#TO DO: add documentation on this script
"""
import pandas as pd
import requests
from rdflib import Graph

TPF_DBPEDIA = \
    "https://api.triplydb.com/datasets/DBpedia-association/snapshot-2021-09/fragments/?limit=10000"

class TriplInterface:
    """
    #TO DO: add documentation on this script
    """

    def __init__(self, default_pred: list[str],
                 url: str = TPF_DBPEDIA):
        # Former url: "https://api.triplydb.com/datasets/DBpedia-association/dbpedia/fragments"
        self.url = url
        self.headers = {
            'Accept': 'application/trig'
        }
        self.format = "trig"
        self.pred = default_pred

    def _run_get_request(self, params: dict[str, str]):
        """ Retrieving get curl request by chunks """
        content = bytes('', 'utf-8')
        with requests.get(self.url, headers=self.headers,
                          params=params, timeout=10,
                          stream=True) as response:
            response.raise_for_status()
            for chunk in response.iter_content(chunk_size=8192):
                content += chunk
        return content

    def run_curl_request(self, params: dict[str, str], filter_pred: list,
                          filter_keep: bool):
        """ Returning triples corresponding to query """
        # response = requests.get(self.url, headers=self.headers,
        #                         params=params, timeout=10)
        content = self._run_get_request(params)
        graph = Graph().parse(data=content, format=self.format)
        # graph = Graph().parse(data=response.content, format=self.format)
        if filter_keep:
            return [(a, b, c) for (a, b, c) in graph if str(b) in filter_pred]
        return [(a, b, c) for (a, b, c) in graph if str(b) not in filter_pred]

    def get_superclass(self, node):
        """ Superclass of a node
        Most ancient ancestor before owl:Thing """
        info = self.run_curl_request(
            params=dict(subject=str(node)),
            filter_pred=["http://www.w3.org/2000/01/rdf-schema#subClassOf"],
            filter_keep=True)

        if not info:
            return node
        if str(info[0][2]) == "http://www.w3.org/2002/07/owl#Thing":
            return node
        return self.get_superclass(str(info[0][2]))

    def _get_all_results(self, node: str, predicate: list[str]):

        ingoing = self._get_ingoing(node, predicate)
        outgoing = self._get_outgoing(node, predicate)
        return ingoing, outgoing, self._get_type(nodes=ingoing+outgoing)

    def _get_ingoing(self, node: str, predicate: list[str]):
        """ Return all triples (s, p, o) s.t.
        p not in predicate and o = node """
        return self.run_curl_request(params=dict(object=str(node)),
                                     filter_pred=predicate, filter_keep=False)

    def _get_outgoing(self, node: str, predicate: list[str]):
        """ Return all triples (s, p, o) s.t.
        p not in predicate and s = node """
        return self.run_curl_request(params=dict(subject=str(node)),
                                     filter_pred=predicate, filter_keep=False)

    def _get_type(self, nodes: list[str]):
        temp_res = []

        for i, (subject, _, _) in enumerate(nodes):
            print(f"Processing subject {i+1}/{len(nodes)}")
            temp_res += self.run_curl_request(params=dict(subject=str(subject)),
                                               filter_pred=self.pred,
                                               filter_keep=True)
        return temp_res

    @staticmethod
    def _get_df(list_triples: list[tuple]) -> pd.core.frame.DataFrame:
        return pd.DataFrame({"subject": [row[0] for row in list_triples],
                             "predicate": [row[1] for row in list_triples],
                             "object": [row[2] for row in list_triples]}).drop_duplicates()

    def __call__(self, node: str, predicate: list[str]) -> pd.core.frame.DataFrame:
        ingoing, outgoing, types = self._get_all_results(node=node, predicate=predicate)
        return self._get_df(ingoing), self._get_df(outgoing), self._get_df(types)



if __name__ == '__main__':
    NODE = "http://dbpedia.org/resource/French_Revolution"
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

    interface = TriplInterface(default_pred=["http://www.w3.org/1999/02/22-rdf-syntax-ns#type"])
    ingoing_test, outgoing_test, types_test = interface(node=NODE, predicate=PREDICATE)
    print(f"{ingoing_test}\n{outgoing_test}\n{types_test}")

    # import os
    # from settings import FOLDER_PATH
    # folder = os.path.join(FOLDER_PATH, "src/tests/")
    # ingoing_test.to_csv(f"{folder}triply_ingoing_expected.csv")
    # outgoing_test.to_csv(f"{folder}triply_outgoing_expected.csv")
    # types_test.to_csv(f"{folder}triply_types_expected.csv")
