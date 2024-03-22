# -*- coding: utf-8 -*-
"""
All info related to ConceptNet

API + local version
"""
from typing import Union, List
import requests
from tqdm import tqdm
import pandas as pd

def extract_triples(response, lang: Union[None, str]):
    """ Extracting triples from API output """
    triples = [(x['start']['@id'], x['rel']['@id'], x['end']['@id']) for x in response["edges"]]
    if lang:
        triples = [x for x in triples if f"/{lang}/" in x[0] and f"/{lang}/" in x[1]]
    return pd.DataFrame(triples, columns=["subject", "predicate", "object"])

def helper_filtering(x, labels):
    """ Helper for df filtering """
    return any(x.startswith(label) for label in labels)

class ConceptNet:
    """ Class related to ConceptNet 
    Usage to API is more direct but 
    (1) constrained wrt the number of queries you can make
    (2) lesser information than with local version

    We strongly advise to download the ConceptNet data from this link:
    https://github.com/commonsense/conceptnet5/wiki/Downloads

    We further filtered English ConceptNet with this command line:
    ```bash
    grep "\\].*\\/c\\/en\\/.*\\/c\\/en\\/.*{.*}" assertions.csv > filtered_assertions.csv
    ```

    api: 'http://api.conceptnet.io/'
    """
    def __init__(self, api: Union[str, None] = None,
                 cn_csv: Union[str, None] = None):
        self.api = api
        self.cn_csv = cn_csv
        self.check_args()

        if cn_csv:
            self.cn_local = pd.read_csv(cn_csv, sep="\t", header=None)
            self.cn_local = self.cn_local[[col for col in self.cn_local.columns \
                if col != "Unnamed: 0"]]
            self.cn_local.columns = ["triple_id", "predicate", "subject", "object", "metadata"]
        else:
            self.cn_local = False

        self.method = "api" if self.api else "local"

    def check_args(self):
        """ api and cn_csv is a xor """
        if not (self.api is None) ^ (self.cn_csv is None):
            raise ValueError("You can only query ConceptNet either through " + \
                "the API or (as xor) through the downloaded CSV")

    @staticmethod
    def run_api(template):
        """ Get requests """
        return requests.get(template, timeout=3600).json()

    def get_ingoing_concept_api(self, word, lang: str = "en"):
        """ Ingoing edges from a concept
        Return the raw json output and reusable triples """
        template = self.api + f"query?end=/c/{lang}/{word}"
        response = self.run_api(template=template)
        return response, extract_triples(response=response, lang=lang)

    def get_outgoing_concept_api(self, word, lang: str = "en"):
        """ Ingoing edges from a concept
        Return the raw json output and reusable triples """
        template = self.api + f"query?start=/c/{lang}/{word}"
        response = self.run_api(template=template)
        return response, extract_triples(response=response, lang=lang)

    def get_concepts_api(self, labels, lang: str = "en"):
        """ Save concepts from list
        Main limitation = time, setting time sleepers to respect API limit
        (3600 requests/hour, with a burst of 120 requests per minute) """
        res = pd.DataFrame(columns=["subject", "predicate", "object"])

        for label in tqdm(labels):
            _, ingoing = self.get_ingoing_concept_api(word=label, lang=lang)
            _, outgoing = self.get_outgoing_concept_api(word=label, lang=lang)
            res = pd.concat([res, ingoing, outgoing])
        return res

    def get_concepts_local(self, labels: List[str], entity: bool,
                           relation: bool, lang: str = "en"):
        """ Retrieving concepts from local """ 
        columns = ["subject", "predicate", "object"]
        if entity:
            labels = [f"/c/{lang}/{x}/" for x in labels]
            filter_df = (self.cn_local.subject.apply(lambda x: helper_filtering(x, labels))) | \
                (self.cn_local.subject.apply(lambda x: helper_filtering(x, labels)))
            return self.cn_local[filter_df][columns]
        if relation:
            labels = [f"/c/{lang}/{x}/" for x in labels]
            # labels = [f"/r/{x}" for x in labels]
            # filter_df = self.cn_local.predicate.apply(lambda x: helper_filtering(x, labels))
            filter_df = (self.cn_local.subject.apply(lambda x: helper_filtering(x, labels))) | \
                (self.cn_local.subject.apply(lambda x: helper_filtering(x, labels)))
            return self.cn_local[filter_df][columns]
        return pd.DataFrame(columns=columns)

    def get_n_hop_neighbours(self, node, n: int, lang: str = "en"):
        """ N-hop neighbours around graph """
        res = pd.DataFrame(columns=["subject", "predicate", "object"])
        iteration = 1
        labels = [node]
        visited = []
        while iteration <= n:
            curr_df = self(labels=labels, lang=lang, entity=True, relation=False)
            res = pd.concat([res, curr_df])
            visited += labels
            labels = [x.replace(f"/c/{lang}/", "").split("/")[0] \
                for x in curr_df.subject.unique().tolist() + curr_df.object.unique().tolist() \
                    if x not in visited]
            iteration += 1
        return res

    def __call__(self, labels, lang: Union[None, str] = "en",
                 entity: Union[bool, None] = None, relation: Union[bool, None] = None):
        """ 
        Labels is a list of human-readable labels to be queries in ConceptNet
        They need to be adjusted for querying """
        if self.method == "local" and not entity ^ relation:
            raise ValueError("If using ConceptNet locally, you need to specify " + \
                "either entity xor relation as True")

        aggregator = "+" if self.method == "api" else "_"
        labels = [aggregator.join(label.split()) for label in labels]
        if self.method == "api":
            return self.get_concepts_api(labels=labels, lang=lang)
        return self.get_concepts_local(labels=labels, entity=entity,
                                        relation=relation, lang=lang)



if __name__ == '__main__':
    CN_CSV = "./data/concept_net/filtered_assertions.csv"
    CONCEPT_NET = ConceptNet(api=None, cn_csv=CN_CSV)

    # R, TRIPLES = CONCEPT_NET.get_ingoing_concept(word=WORD, lang=LANG)
    # print(TRIPLES)
    # R, TRIPLES = CONCEPT_NET.get_outgoing_concept(word=WORD, lang=LANG)
    # print(TRIPLES)

    LABELS = ["lost"]
    OUTPUT = CONCEPT_NET(labels=LABELS, lang="en", entity=True, relation=False)
    OUTPUT.to_csv("test.csv")
    print(OUTPUT)

    # ONE_HOP = CONCEPT_NET.get_n_hop_neighbours(node="murder", n=2, lang="en")
    # ONE_HOP.to_csv("neighbours.csv")
