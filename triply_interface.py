import pandas as pd
import requests
from rdflib.term import URIRef
from rdflib import Graph

class TriplInterface:
    
    def __init__(self, url: str = "https://api.triplydb.com/datasets/DBpedia-association/snapshot-2021-09/fragments"):
        # Former url: "https://api.triplydb.com/datasets/DBpedia-association/dbpedia/fragments"
        self.url = url
        self.headers = {
            'Accept': 'application/trig',
        }
        self.format = "trig"
        self.default_pred_filters = list()
        self.default_pred_filters = ["http://dbpedia.org/ontology/wikiPageWikiLink"]
        self.pred = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
    
    def _run_curl_request(self, params: dict[str, str], filter: list = list()):
        response = requests.get(self.url, headers=self.headers,
                                params=params)
        g = Graph().parse(data=response.content, format=self.format)
        return [(a, b, c) for (a, b, c) in g if str(b) not in filter]
    
    def _get_all_results(self, node: str, predicate: list[str]):
        results = self._run_curl_request(params=dict(object=str(node)),
                                         filter=self.default_pred_filters + predicate)
        print(results)
        temp_res = list()
        for _, (subject, _, _) in enumerate(results):
            # print(f"Processing subject {i+1}/{len(results)}")
            temp_res += self._run_curl_request(params=dict(subject=str(subject),
                                                          predicate=self.pred))
        return results + temp_res
    
    
    
    
    
    def __call__(self, node: str, predicate: list[str]) -> pd.core.frame.DataFrame:
        results = self._get_all_results(node=node, predicate=predicate)
        return pd.DataFrame({"subject": [row[0] for row in results],
                             "predicate": [row[1] for row in results],
                             "object": [row[2] for row in results]}).drop_duplicates()


if __name__ == '__main__':
    node = "http://dbpedia.org/resource/Storming_of_the_Bastille"
    predicate = ["http://dbpedia.org/ontology/wikiPageRedirects"]
    predicate = list()

    interface = TriplInterface()
    df = interface(node=node, predicate=predicate)
    # df.to_csv("triply.csv")
    print(df)