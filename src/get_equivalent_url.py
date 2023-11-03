# -*- coding: utf-8 -*-
"""
URI might change from one version of a dataset to another
(e.g. DBPedia 2016-10 --> DBPedia 2021-09)
Using dbo:wikiPageRedirects to find equivalent URIs
"""
import json
from tqdm import tqdm

import pandas as pd
from src.triply_interface import TriplInterface

def get_equivalent_url(df_path: str, save_path: str, dataset: str = "dbpedia"):
    """
    df_path contains the ground truth, i.e. the events for a given topic
    The "linkDBpediaEn" column must contain the dbpedia corresponding pages
    """
    df_pd = pd.read_csv(df_path)
    if dataset == "dbpedia":
        urls = df_pd.linkDBpediaEn.unique()
        referents = {}
        predicate = ["http://dbpedia.org/ontology/wikiPageRedirects"]

        interface = TriplInterface()

        for i in tqdm(range(len(urls))):
            url = urls[i]
            referents[url] = url
            triples = interface.run_request(params=dict(object=url), filter_pred=predicate,
                                            filter_keep=True)
            for (ref, _, _) in triples:
                referents[str(ref)] = url

        json.dump(referents, open(save_path, "w", encoding='utf-8'),
                indent=4)

    else:
        referents = {event: event for event in df_pd.linkDBpediaEn.unique()}
        json.dump(referents, open(save_path, "w", encoding='utf-8'),
                  indent=4)


if __name__ == '__main__':
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("-df", "--df_path", required=True,
                    help="csv path to extract referent events from")
    ap.add_argument("-j", "--json", required=True,
                    help="json path to save the referents to")
    args = vars(ap.parse_args())

    get_equivalent_url(df_path=args["df_path"], save_path=args["json"])
