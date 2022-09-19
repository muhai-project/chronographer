"""
#TO DO: add documentation on this script
"""
import os
import fnmatch
from tqdm import tqdm
import yaml

import pandas as pd
from hdt import HDTDocument

from settings import FOLDER_PATH

HDT_DBPEDIA = \
    os.path.join(FOLDER_PATH, "dbpedia-snapshot-2021-09")

DEFAULT_PRED = \
    ["http://www.w3.org/1999/02/22-rdf-syntax-ns#type",
     "http://dbpedia.org/ontology/date",
     "http://dbpedia.org/ontology/startDate",
     "http://dbpedia.org/ontology/endDate",
     "http://dbpedia.org/property/birthDate",
     "http://dbpedia.org/property/deathDate"]

with open(os.path.join(FOLDER_PATH, "dataset-config", "dbpedia.yaml"),
          encoding='utf-8') as file:
    dbpedia_dataset_config = yaml.load(file, Loader=yaml.FullLoader)

class HDTInterface:
    """
    #TO DO: add documentation on this script
    """

    def __init__(self, dataset_config: dict = dbpedia_dataset_config,
                 dates: list[str] = [None, None], default_pred: list[str] = DEFAULT_PRED,
                 folder_hdt: str = HDT_DBPEDIA, nested_dataset: bool = True, filter_kb: bool = 1):
        if nested_dataset:
            dirs = [os.path.join(folder_hdt, file) for file in os.listdir(folder_hdt)]
            dirs = [elt for elt in dirs if not elt.split('/')[-1].startswith(".")]
            dirs = [os.path.join(old_dir, new_dir, "hdt") \
                for old_dir in dirs for new_dir in os.listdir(old_dir)]
            dirs = [elt for elt in dirs if not elt.split('/')[-2].startswith(".")]
            self.docs = [HDTDocument(file) for file in dirs]
        else:
            files = [os.path.join(folder_hdt, file) for file in os.listdir(folder_hdt) \
                        if fnmatch.fnmatch(file, "*.hdt")]
            self.docs = [HDTDocument(file) for file in files]

        self.pred = default_pred

        self.start_date = dates[0]
        self.end_date = dates[1]
        self.filter_kb = filter_kb

        self.dataset_config = dataset_config
        self.dataset_type = dataset_config["config_type"]

    def run_request(self, params: dict[str, str], filter_pred: list,
                          filter_keep: bool):
        """ Returning triples corresponding to query """
        subject_t = params["subject"] if "subject" in params else ""
        predicate_t = params["predicate"] if "predicate" in params else ""
        object_t = params["object"] if "object" in params else ""

        triples = []
        for doc in self.docs:
            curr_triples, _ = doc.search_triples(subject_t, predicate_t, object_t)
            triples += list(curr_triples)

        if filter_keep:
            return [(a, b, c) for (a, b, c) in triples if b in filter_pred]
        return [(a, b, c) for (a, b, c) in triples if b not in filter_pred]

    def get_superclass(self, node):
        """ Superclass of a node
        Most ancient ancestor before owl:Thing """
        info = self.run_request(
            params=dict(subject=str(node)),
            filter_pred=self.dataset_config["sub_class_of"],
            filter_keep=True)

        if not info:
            return node
        if str(info[0][2]) == self.dataset_config["owl_thing"]:
            return node
        return self.get_superclass(str(info[0][2]))

    def _get_all_results(self, node: str, predicate: list[str]):

        ingoing = self._get_ingoing(node, predicate)
        outgoing = self._get_outgoing(node, predicate)
        return ingoing, outgoing, self._filter_specific(
            self._get_specific_outgoing(ingoing=ingoing, outgoing=outgoing))

    def _filter_namespace(self, triples):
        # to_discard = [
        #     "http://en.wikipedia.org/", "https", "http://citation.dbpedia.org/",
        #     "http://books.google.com/", "http://en.wikisource", "http://www.sparknotes.com", '"',
        #     "http://whc.unesco.org", "http://www", "http://dinlarthelwa",
        #     "http://afm", "http://news.bbc.co.uk", "http://hsbc.wimbledon.com"
        # ]

        # triples = [elt for elt in triples if \
        #     not any(elt[2].startswith(discard) for discard in to_discard)]
        # triples = [elt for elt in triples if \
        #     not any(elt[0].startswith(discard) for discard in to_discard)]

        filter_f = lambda x: x.startswith(self.dataset_config["start_uri"]) or \
                            not any(x.startswith(elt) for elt in ["http", '"'])

        triples = [elt for elt in triples if filter_f(elt[2])]
        triples = [elt for elt in triples if filter_f(elt[0])]

        return triples


    @staticmethod
    def pre_process_date(x_date):
        """ Pre processing date (to be format comparable later) """
        xml_dates = [
            "<http://www.w3.org/2001/XMLSchema#date>",
            "<http://www.w3.org/2001/XMLSchema#dateTime>"
        ]
        if any(xml_date in x_date for xml_date in xml_dates):
            return x_date[1:11]
        elif "<http://www.w3.org/2001/XMLSchema#integer>" in x_date:
            return x_date[1:5]
        else:
            return x_date

    def _filter_node(self, triples, filter_out):
        triples = self._filter_namespace(triples)
        triples = [elt for elt in triples if \
            not any(elt[0].startswith(pattern) for pattern in filter_out)]
        triples = [elt for elt in triples if \
            not any(elt[2].startswith(pattern) for pattern in filter_out)]
        return triples

    def _filter_specific(self, triples):
        invalid = ['"Unknown"@']
        triples = [(sub, pred, obj) for (sub, pred, obj) in triples if obj not in invalid]
        return [(sub, pred, self.pre_process_date(obj)) for (sub, pred, obj) in triples]

    def _get_outgoing(self, node: str, predicate: list[str]):
        """ Return all triples (s, p, o) s.t.
        p not in predicate and s = node """
        return self._helper_ingoing_outgoing(params=dict(subject=str(node)),
                                             predicate=predicate, filter_keep=False)

    def _get_ingoing(self, node: str, predicate: list[str]):
        """ Return all triples (s, p, o) s.t.
        p not in predicate and o = node """
        return self._helper_ingoing_outgoing(params=dict(object=str(node)),
                                             predicate=predicate, filter_keep=False)

    def _helper_ingoing_outgoing(self, params, predicate, filter_keep):
        triples = self.run_request(params=params,
                                   filter_pred=predicate, filter_keep=filter_keep)
        if self.filter_kb  and self.dataset_config['config_type'] == "dbpedia":
            return self._filter_node(triples=triples, filter_out=[self.dataset_config["category"]])
        if self.dataset_config['config_type'] == "wikidata":
            return self._filter_node(triples=triples,
                                     filter_out=self.dataset_config["start_stop_uri"])
        return triples

    def _get_specific_outgoing(self, ingoing: list[tuple], outgoing: list[tuple]):
        temp_res = []

        for i in tqdm(range(len(ingoing))):
            subject = ingoing[i][0]
            temp_res += self.run_request(params=dict(subject=str(subject)),
                                             filter_pred=self.pred,
                                             filter_keep=True)

        for i in tqdm(range(len(outgoing))):
            object_t = outgoing[i][2]
            temp_res += self.run_request(params=dict(subject=str(object_t)),
                                                         filter_pred=self.pred,
                                                         filter_keep=True)

        return temp_res

    @staticmethod
    def _get_df(list_triples: list[tuple], type_df: str) -> pd.core.frame.DataFrame:
        return pd.DataFrame({"subject": [str(row[0]) for row in list_triples],
                             "predicate": [str(row[1]) for row in list_triples],
                             "object": [str(row[2]) for row in list_triples],
                             "type_df": [type_df] * len(list_triples)}).drop_duplicates()

    def __call__(self, node: str, predicate: list[str]) -> pd.core.frame.DataFrame:
        ingoing, outgoing, types = self._get_all_results(node=node, predicate=predicate)
        return self._get_df(ingoing, type_df="ingoing"), \
            self._get_df(outgoing, type_df="outgoing"), \
            self._get_df(types, type_df="spec. outgoing")



if __name__ == '__main__':
    NODE = "http://dbpedia.org/resource/André_Masséna"
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
                    "http://www.w3.org/ns/prov#wasDerivedFrom",
                    "http://dbpedia.org/ontology/wikiPageWikiLinkText",
                    "http://dbpedia.org/ontology/wikiPageOutDegree",
                    "http://dbpedia.org/ontology/abstract",
                    "http://www.w3.org/2000/01/rdf-schema#comment",
                    "http://www.w3.org/2000/01/rdf-schema#label"]

    interface = HDTInterface()
    ingoing_test, outgoing_test, types_test = interface(node=NODE, predicate=PREDICATE)
    print(f"{ingoing_test}\n{outgoing_test}\n{types_test}")

    ingoing_test.to_csv(f"{FOLDER_PATH}/hdt_ingoing.csv")
    outgoing_test.to_csv(f"{FOLDER_PATH}/hdt_outgoing.csv")
    types_test.to_csv(f"{FOLDER_PATH}/hdt_types.csv")
