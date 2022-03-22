"""
Unittest of file `expansion.py`, class TriplInterface
python -m unittest -v test_expansion.py
"""

import os
import unittest
from unittest.mock import create_autospec
from rdflib.term import URIRef

import pandas as pd
from settings import FOLDER_PATH
from src.expansion import NodeExpansion
from src.triply_interface import TriplInterface
from src.hdt_interface import HDTInterface

def clean_df(df_pd):
    """ Keeping 3 columns """
    return df_pd[['subject', 'object', 'predicate']]

class TestNodeExpansion(unittest.TestCase):
    """
    Test class for Expansion class
    """

    def test_init_rdf_type(self):
        """ Test __init__: checking param `rdf_type`  """
        interface = TriplInterface(default_pred=["http://www.w3.org/1999/02/22-rdf-syntax-ns#type"])

        # Working
        rdf_type =  [("event", "http://dbpedia.org/ontology/Event"),
                     ("person", "http://dbpedia.org/ontology/Person")]

        try:
            NodeExpansion(rdf_type=rdf_type,
                          interface=interface,
                          args_filtering={"where": 1, "when": 1})
        except ValueError as error:
            print(error)
            self.fail("NodeExpansion()raised ValueError unexpectedly")

        # Not working
        for rdf_type in [[], "test", [(1, 2, 3), (4, 5, 6)]]:
            with self.assertRaises(ValueError):
                NodeExpansion(rdf_type=rdf_type,
                              args_filtering={"where": 1, "when": 1},
                              interface=interface)

    def test_init_interface(self):
        """ Test __init__: checking param `interface`  """
        rdf_type =  [("event", URIRef("http://dbpedia.org/ontology/Event"))]

        # Working
        for interface in [TriplInterface(
            default_pred=["http://www.w3.org/1999/02/22-rdf-syntax-ns#type"]), \
                          HDTInterface()]:
            try:
                NodeExpansion(rdf_type=rdf_type,
                              args_filtering={"where": 1, "when": 1},
                              interface=interface)
            except ValueError as error:
                print(error)
                self.fail("NodeExpansion()raised ValueError unexpectedly")

        # Not working
        with self.assertRaises(ValueError):
            NodeExpansion(rdf_type=rdf_type,
                          args_filtering={"where": 1, "when": 1},
                          interface="test")

    def test_filter_sub_graph(self):
        """ Test get_output_triples """
        folder = os.path.join(FOLDER_PATH, "src/tests/data/")
        ingoing_expected = clean_df(pd.read_csv(f"{folder}triply_ingoing_expected.csv"))
        outgoing_expected = clean_df(pd.read_csv(f"{folder}triply_outgoing_expected.csv"))
        types_expected = clean_df(pd.read_csv(f"{folder}triply_types_expected.csv"))

        rdf_type =  [("event", "http://dbpedia.org/ontology/Event")]
        interface = create_autospec(TriplInterface(
            default_pred=["http://www.w3.org/1999/02/22-rdf-syntax-ns#type"]))
        interface.return_value = (ingoing_expected, outgoing_expected, types_expected)

        expansion = NodeExpansion(rdf_type=rdf_type,
                                  args_filtering={"where": 1, "when": 1},
                                  interface=interface)
        ingoing, outgoing, types = expansion.get_output_triples(node=None, predicate=None)

        to_discard = expansion.filtering(df_pd=types, dates=["1789-01-01", "1804-12-31"])
        to_keep = list(types[(~types.subject.isin(to_discard)) & \
            (types.object.isin(list(expansion.mapping.keys())))].subject.unique())

        subgraph_ingoing_expected = ingoing[ingoing.subject.isin(to_keep)]
        path_ingoing_expected = ingoing[~ingoing.subject.isin(to_discard)]
        subgraph_outgoing_expected = outgoing[outgoing.object.isin(to_keep)]
        path_outgoing_expected = outgoing[~outgoing.object.isin(to_discard)]


        subgraph_ingoing, path_ingoing, subgraph_outgoing, path_outgoing, _ = \
            expansion.filter_sub_graph(types_expected, ingoing_expected, outgoing_expected,
                                       ["1789-01-01", "1804-12-31"])

        for df1, df2 in [(subgraph_ingoing, subgraph_ingoing_expected),
                           (path_ingoing, path_ingoing_expected),
                           (subgraph_outgoing, subgraph_outgoing_expected),
                           (path_outgoing, path_outgoing_expected)]:
            merged = df1.merge(df2, how='left', on=["subject", "object", "predicate"])
            self.assertTrue(merged.shape == df1.shape)
            self.assertTrue(merged.shape == df2.shape)
