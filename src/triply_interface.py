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

    def __init__(self, url: str = TPF_DBPEDIA):
        # Former url: "https://api.triplydb.com/datasets/DBpedia-association/dbpedia/fragments"
        self.url = url
        self.headers = {
            'Accept': 'application/trig'
        }
        self.format = "trig"
        self.pred = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"

    def _run_curl_request(self, params: dict[str, str], filter_pred: list,
                          filter_keep: bool):
        response = requests.get(self.url, headers=self.headers,
                                params=params, timeout=10)
        graph = Graph().parse(data=response.content, format=self.format)
        if filter_keep:
            return [(a, b, c) for (a, b, c) in graph if str(b) in filter_pred]
        else:
            return [(a, b, c) for (a, b, c) in graph if str(b) not in filter_pred]

    def _get_all_results(self, node: str, predicate: list[str]):
        results = self._run_curl_request(params=dict(object=str(node)),
                                         filter_pred=predicate,
                                         filter_keep=False)
        temp_res = []
        for i, (subject, _, _) in enumerate(results):
            print(f"Processing subject {i+1}/{len(results)}")
            temp_res += self._run_curl_request(params=dict(subject=str(subject)),
                                               filter_pred=self.pred,
                                               filter_keep=True)
        return results + temp_res


    def __call__(self, node: str, predicate: list[str]) -> pd.core.frame.DataFrame:
        results = self._get_all_results(node=node, predicate=predicate)
        return pd.DataFrame({"subject": [row[0] for row in results],
                             "predicate": [row[1] for row in results],
                             "object": [row[2] for row in results]}).drop_duplicates()


if __name__ == '__main__':
    NODE = "http://dbpedia.org/resource/Category:French_Revolution"
    PREDICATE = ["http://dbpedia.org/ontology/wikiPageWikiLink",
                 "http://dbpedia.org/ontology/wikiPageRedirects"]
    # PREDICATE = []

    interface = TriplInterface()
    df = interface(node=NODE, predicate=PREDICATE)
    df.sort_values(by=['predicate', 'subject']).to_csv("triply.csv")
