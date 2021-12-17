"""
Unittest of file `sparql_interface.py`, class TriplInterface
python -m unittest -v test_sparql_interface.py
"""

import unittest
import pandas as pd
from src.sparql_interface import SPARQLInterface

class TestSPARQLInterface(unittest.TestCase):
    """
    Test class
    """

    def test_call(self):
        """ Test __call__ """
        node = "http://dbpedia.org/resource/Category:French_Revolution"
        predicate = []
        interface = SPARQLInterface()
        dataframe = interface(node=node, predicate=predicate) \
            [['subject', 'object', 'predicate']] \
                .sort_values(by=['subject', 'object', 'predicate']) \
                    .reset_index(drop=True)

        df_expected = pd.read_csv("./sparql_expected.csv") \
            [['subject', 'object', 'predicate']] \
                .sort_values(by=['subject', 'object', 'predicate']) \
                    .reset_index(drop=True)

        self.assertTrue(\
            pd.concat([dataframe,df_expected]) \
                .drop_duplicates(keep=False).shape[0] == 0)
