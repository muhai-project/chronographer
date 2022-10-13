"""
Unittest of file `hdt_interface.py`, class HDTInterface
python -m unittest -v test_hdt_interface.py
"""

import os
import json
import unittest
import pandas as pd
from settings import FOLDER_PATH
from src.hdt_interface import HDTInterface

with open(os.path.join(
    FOLDER_PATH, "sample-data", "French_Revolution_referents.json"),
    encoding="utf-8") as openfile:
    REFERENTS = json.load(openfile)

def reorder_df(df_pd):
    """ Reordering df rows and columns for comparison """
    df_pd = df_pd[['subject', 'object', 'predicate', 'type_df']] \
                .sort_values(by=['subject', 'object', 'predicate', 'type_df']) \
                    .reset_index(drop=True)
    return df_pd


class TestHDTInterface(unittest.TestCase):
    """
    Test class for HDT Interface (dbpedia 2016-10)
    """

    def test_call(self):
        """ Test __call__ """
        node = "http://dbpedia.org/resource/French_Revolution"
        predicate = ["http://dbpedia.org/ontology/wikiPageWikiLink",
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
        ingoing, outgoing, types = interface(node=node, predicate=predicate)
        ingoing, outgoing, types = \
            reorder_df(ingoing), reorder_df(outgoing), reorder_df(types)

        folder = os.path.join(FOLDER_PATH, "src/tests/data/")
        ingoing_expected = reorder_df(pd.read_csv(f"{folder}hdt_ingoing_expected.csv"))
        outgoing_expected = reorder_df(pd.read_csv(f"{folder}hdt_outgoing_expected.csv"))
        types_expected = reorder_df(pd.read_csv(f"{folder}hdt_types_expected.csv"))

        for (df1, df2) in [(ingoing, ingoing_expected), (outgoing, outgoing_expected),
                           (types, types_expected)]:
            merged = df1.merge(df2, how='left', on=["subject", "object", "predicate", "type_df"])
            self.assertTrue(merged.shape == df1.shape)
            self.assertTrue(merged.shape == df2.shape)
