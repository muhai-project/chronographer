# -*- coding: utf-8 -*-
"""
Unittest of file `triply_interface.py`, class TriplInterface
python -m unittest -v test_triply_interface.py
"""

import os
import unittest
import pandas as pd
from settings import FOLDER_PATH
from src.triply_interface import TriplInterface

def reorder_df(df_pd):
    """ Reordering df rows and columns for comparison """
    return df_pd[['subject', 'object', 'predicate', 'type_df']] \
                .sort_values(by=['subject', 'object', 'predicate', 'type_df']) \
                    .reset_index(drop=True)

class TestTriplInterface(unittest.TestCase):
    """
    Test class for Triply DB Interface (dbpedia 2021-09)
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
                    "http://www.w3.org/ns/prov#wasDerivedFrom"]
        interface = TriplInterface(default_pred=["http://www.w3.org/1999/02/22-rdf-syntax-ns#type"])
        ingoing, outgoing, types = interface(node=node, predicate=predicate)
        ingoing, outgoing, types = reorder_df(ingoing), reorder_df(outgoing), reorder_df(types)

        folder = os.path.join(FOLDER_PATH, "src/tests/data/")
        ingoing_expected = reorder_df(pd.read_csv(f"{folder}triply_ingoing_expected.csv"))
        outgoing_expected = reorder_df(pd.read_csv(f"{folder}triply_outgoing_expected.csv"))
        types_expected = reorder_df(pd.read_csv(f"{folder}triply_types_expected.csv"))

        for (df1, df2) in [(ingoing, ingoing_expected), (outgoing, outgoing_expected),
                           (types, types_expected)]:
            merged = df1.merge(df2, how='left', on=["subject", "object", "predicate", "type_df"])

            self.assertTrue(merged.shape == df1.shape)
            self.assertTrue(merged.shape == df2.shape)
