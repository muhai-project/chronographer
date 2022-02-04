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
from src.sparql_interface import SPARQLInterface

def clean_df(df_pd):
    """ Keeping 3 columns """
    return df_pd[['subject', 'object', 'predicate']]

class TestNodeExpansion(unittest.TestCase):
    """
    Test class for Expansion class
    """

    def test_init_rdf_type(self):
        """ Test __init__: checking param `rdf_type`  """
        iteration = 10
        interface = TriplInterface(default_pred=["http://www.w3.org/1999/02/22-rdf-syntax-ns#type"])

        # Working
        rdf_type =  [("event", URIRef("http://dbpedia.org/ontology/Event")),
                     ("person", URIRef("http://dbpedia.org/ontology/Person"))]

        try:
            NodeExpansion(rdf_type=rdf_type, iteration=iteration, interface=interface)
        except ValueError as error:
            print(error)
            self.fail("NodeExpansion()raised ValueError unexpectedly")

        # Not working
        for rdf_type in [[], "test", [(1, 2, 3), (4, 5, 6)], [("hello", "test")]]:
            with self.assertRaises(ValueError):
                NodeExpansion(rdf_type=rdf_type, iteration=iteration, interface=interface)

    def test_init_iteration(self):
        """ Test __init__: checking param `iteration`  """
        rdf_type =  [("event", URIRef("http://dbpedia.org/ontology/Event"))]
        interface = TriplInterface(default_pred=["http://www.w3.org/1999/02/22-rdf-syntax-ns#type"])

        # Working
        try:
            NodeExpansion(rdf_type=rdf_type, iteration=10, interface=interface)
        except ValueError as error:
            print(error)
            self.fail("NodeExpansion()raised ValueError unexpectedly")

        # Not working
        for iteration in [None, "test"]:
            with self.assertRaises(ValueError):
                NodeExpansion(rdf_type=rdf_type, iteration=iteration, interface=interface)

    def test_init_interface(self):
        """ Test __init__: checking param `interface`  """
        rdf_type =  [("event", URIRef("http://dbpedia.org/ontology/Event"))]
        iteration = 10

        # Working
        for interface in [TriplInterface(
            default_pred=["http://www.w3.org/1999/02/22-rdf-syntax-ns#type"]), \
                          SPARQLInterface()]:
            try:
                NodeExpansion(rdf_type=rdf_type, iteration=10, interface=interface)
            except ValueError as error:
                print(error)
                self.fail("NodeExpansion()raised ValueError unexpectedly")

        # Not working
        with self.assertRaises(ValueError):
            NodeExpansion(rdf_type=rdf_type, iteration=iteration, interface="test")

    def test_filter_sub_graph(self):
        """ Test get_output_triples """
        folder = os.path.join(FOLDER_PATH, "src/tests/")
        ingoing_expected = clean_df(pd.read_csv(f"{folder}triply_ingoing_expected.csv"))
        outgoing_expected = clean_df(pd.read_csv(f"{folder}triply_outgoing_expected.csv"))
        types_expected = clean_df(pd.read_csv(f"{folder}triply_types_expected.csv"))

        rdf_type =  [("event", URIRef("http://dbpedia.org/ontology/Event"))]
        iteration = 10
        interface = create_autospec(TriplInterface(
            default_pred=["http://www.w3.org/1999/02/22-rdf-syntax-ns#type"]))
        interface.return_value = (ingoing_expected, outgoing_expected, types_expected)

        expansion = NodeExpansion(rdf_type=rdf_type, iteration=iteration, interface=interface)
        ingoing, outgoing, types = expansion.get_output_triples(node=None, predicate=None)

        subject = types[types.object.isin(expansion.mapping.keys())] \
            .subject.values
        subgraph_ingoing_expected = ingoing[ingoing.subject.isin(subject)]
        path_ingoing_expected = ingoing[~ingoing.subject.isin(subject)]
        subgraph_outgoing_expected = outgoing[outgoing.object.isin(subject)]
        path_outgoing_expected = outgoing[~outgoing.object.isin(subject)]


        subgraph_ingoing, path_ingoing, subgraph_outgoing, path_outgoing = \
            expansion.filter_sub_graph(types_expected, ingoing_expected, outgoing_expected)

        for df1, df2 in [(subgraph_ingoing, subgraph_ingoing_expected),
                           (path_ingoing, path_ingoing_expected),
                           (subgraph_outgoing, subgraph_outgoing_expected),
                           (path_outgoing, path_outgoing_expected)]:
            merged = df1.merge(df2, how='left', on=["subject", "object", "predicate"])
            self.assertTrue(merged.shape == df1.shape)
            self.assertTrue(merged.shape == df2.shape)
