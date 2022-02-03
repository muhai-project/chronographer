"""
Unittest of file `triply_interface.py`, class TriplInterface
python -m unittest -v test_triply_interface.py
"""

import unittest
import pandas as pd
from src.triply_interface import TriplInterface

class TestTriplInterface(unittest.TestCase):
    """
    Test class for Triply DB Interface
    """

    def test_call(self):
        """ Test __call__ """
        node = "http://dbpedia.org/resource/Category:French_Revolution"
        predicate = ["http://dbpedia.org/ontology/wikiPageWikiLink",
                    "http://dbpedia.org/ontology/wikiPageRedirects"]
        interface = TriplInterface(default_pred=["http://www.w3.org/1999/02/22-rdf-syntax-ns#type"])
        dataframe = interface(node=node, predicate=predicate) \
            [['subject', 'object', 'predicate']] \
                .sort_values(by=['subject', 'object', 'predicate']) \
                    .reset_index(drop=True)

        df_expected = pd.read_csv("./triply_expected.csv") \
            [['subject', 'object', 'predicate']] \
                .sort_values(by=['subject', 'object', 'predicate']) \
                    .reset_index(drop=True)

        print()
        self.assertTrue(\
            pd.concat([dataframe,df_expected]) \
                .drop_duplicates(keep=False).shape[0] == 0)
