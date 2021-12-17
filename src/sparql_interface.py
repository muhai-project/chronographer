"""
#TO DO: add documentation on this script
"""
import urllib.request
import pandas as pd
from SPARQLWrapper import SPARQLWrapper, RDFXML
from settings import AGENT

class SPARQLInterface:
    """
    #TO DO: add documentation on this script
    """
    def __init__(self, sparql_endpoint: str = "http://dbpedia.org/sparql",
                 agent: str = AGENT):
        self.sparql = SPARQLWrapper(sparql_endpoint, agent=agent)
        self.query_template = self._set_query_template()

    def _set_query_template(self) -> str:
        query = """
        PREFIX dbo: <http://dbpedia.org/ontology/>
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        CONSTRUCT {
            ?s1 ?p1 ?o1 .
            ?s1 rdf:type ?type1 .
        }
        WHERE {   
        VALUES ?o1 { <[1]> } 
        ?s1 ?p1 ?o1 .
        ?s1 rdfs:label ?s1Label .
        FILTER langMatches( lang(?s1Label), "en" ) .
        FILTER(?p1 NOT IN (dbo:wikiPageWikiLink)) .
        [2]
        ?s1 rdf:type ?type1 .
            }
        """

        return query

    def _get_predicate_filter(self, predicate: list[str]) -> str:
        return f"FILTER(?p1 IN ({', '.join(predicate)}))"

    def _format_template(self, node: str, predicate: list) -> str:
        final_query = self.query_template
        final_query = final_query.replace("[1]", node)

        filter_type = "" if not predicate else self._get_predicate_filter(predicate=predicate)
        final_query = final_query.replace("[2]", filter_type)
        return final_query

    def __call__(self, node: str, predicate: list[str]) -> pd.core.frame.DataFrame:
        query = self._format_template(node=node, predicate=predicate)
        proxy_support = urllib.request.ProxyHandler({})
        opener = urllib.request.build_opener(proxy_support)
        urllib.request.install_opener(opener)
        self.sparql.setQuery(query)
        self.sparql.setReturnFormat(RDFXML)
        results = self.sparql.query().convert()
        return pd.DataFrame({"subject": [row[0] for row in results],
                             "predicate": [row[1] for row in results],
                             "object": [row[2] for row in results]}).drop_duplicates()


if __name__ == '__main__':
    NODE = "http://dbpedia.org/resource/Storming_of_the_Bastille"
    NODE =  "http://dbpedia.org/resource/Causes_of_the_French_Revolution"
    # predicate = ["<http://dbpedia.org/ontology/wikiPageRedirects>"]
    PREDICATE = list()
    # sparql_endpoint = \
    # "https://api.triplydb.com/datasets/DBpedia-association/dbpedia/services/dbpedia/sparql"

    interface = SPARQLInterface()
    df = interface(node=NODE, predicate=PREDICATE)
    df.to_csv("sparql.csv")
    print(df)
