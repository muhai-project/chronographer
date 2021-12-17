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

class TestNodeExpansion(unittest.TestCase):
    """
    Test class for Expansion class
    """

    def test_init_rdf_type(self):
        """ Test __init__: checking param `rdf_type`  """
        iteration = 10
        interface = TriplInterface()

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
        interface = TriplInterface()

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
        for interface in [TriplInterface(), SPARQLInterface()]:
            try:
                NodeExpansion(rdf_type=rdf_type, iteration=10, interface=interface)
            except ValueError as error:
                print(error)
                self.fail("NodeExpansion()raised ValueError unexpectedly")

        # Not working
        with self.assertRaises(ValueError):
            NodeExpansion(rdf_type=rdf_type, iteration=iteration, interface="test")

    def test_get_output_triples(self):
        """ Test get_output_triples """
        output_interface = pd.read_csv(\
            os.path.join(FOLDER_PATH,
                         "src/tests/triply_expected.csv"))

        rdf_type =  [("event", URIRef("http://dbpedia.org/ontology/Event"))]
        iteration = 10
        interface = create_autospec(TriplInterface())
        interface.return_value = output_interface

        expansion = NodeExpansion(rdf_type=rdf_type, iteration=iteration, interface=interface)

        type_df_expected = output_interface[output_interface.predicate == \
            URIRef("http://www.w3.org/1999/02/22-rdf-syntax-ns#type")]
        path_df_expected = output_interface[output_interface.predicate != \
            URIRef("http://www.w3.org/1999/02/22-rdf-syntax-ns#type")]

        type_df, path_df = expansion.get_output_triples(node=None, predicate=None)

        self.assertTrue(\
            pd.concat([type_df,type_df_expected]) \
                .drop_duplicates(keep=False).shape[0] == 0)
        self.assertTrue(\
            pd.concat([path_df,path_df_expected]) \
                .drop_duplicates(keep=False).shape[0] == 0)

    def test_filter_sub_graph(self):
        """ Test get_output_triples """
        output_interface = pd.read_csv(\
            os.path.join(FOLDER_PATH,
                         "src/tests/triply_expected.csv"))

        rdf_type =  [("event", URIRef("http://dbpedia.org/ontology/Event"))]
        iteration = 10
        interface = create_autospec(TriplInterface())
        interface.return_value = output_interface
        print(type(interface))
        expansion = NodeExpansion(rdf_type=rdf_type, iteration=iteration, interface=interface)
        type_df, path_df = expansion.get_output_triples(node=None, predicate=None)

        subject = type_df[type_df.object.isin(expansion.mapping.keys())] \
            .subject.values
        subgraph_expected = path_df[path_df.subject.isin(subject)]

        subgraph = expansion.filter_sub_graph(type_df, path_df)

        self.assertTrue(\
            pd.concat([subgraph,subgraph_expected]) \
                .drop_duplicates(keep=False).shape[0] == 0)
