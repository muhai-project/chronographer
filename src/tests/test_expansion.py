# -*- coding: utf-8 -*-
"""
Unittest of file `expansion.py`, class NodeExpansion
python -m unittest -v test_expansion.py
"""

import os
import unittest
import yaml

import pandas as pd
from settings import FOLDER_PATH
from src.expansion import NodeExpansion
from src.hdt_interface import HDTInterface

def clean_df(df_pd):
    """ Only 3 cols """
    return df_pd[['subject', 'object', 'predicate']]

def get_args_filtering():
    """ Getting param filtering dict for instantiating NodeExpansion class """
    with open(
        os.path.join(FOLDER_PATH, "dataset-config/dbpedia.yaml"),
        encoding='utf-8') as openfile:
        dataset_config = yaml.load(openfile, Loader=yaml.FullLoader)

    return {
        "when": 1,
        "where": 1,
        "who": 0,
        "point_in_time": dataset_config["point_in_time"],
        "start_dates": dataset_config["start_dates"],
        "end_dates": dataset_config["end_dates"],
        "places": dataset_config["places"],
        "people": dataset_config["person"],
        "dataset_type": dataset_config["config_type"],
    }

ARGS_FILTERING = get_args_filtering()

class TestNodeExpansion(unittest.TestCase):
    """
    Test class for Expansion class
    """

    def test_init_rdf_type(self):
        """ Test __init__: checking param `rdf_type`  """
        interface = HDTInterface()

        # Working
        rdf_type =  [("event", "http://dbpedia.org/ontology/Event"),
                     ("person", "http://dbpedia.org/ontology/Person")]

        try:
            NodeExpansion(rdf_type=rdf_type,
                          interface=interface,
                          args_filtering=ARGS_FILTERING)
        except ValueError as error:
            print(error)
            self.fail("NodeExpansion()raised ValueError unexpectedly")

        # Not working
        for rdf_type in [[(1, 2, 3), (4, 5, 6)]]:
            with self.assertRaises(ValueError):
                NodeExpansion(rdf_type=rdf_type,
                              args_filtering=ARGS_FILTERING,
                              interface=interface)

    def test_init_interface(self):
        """ Test __init__: checking param `interface`  """
        rdf_type =  [("event", "http://dbpedia.org/ontology/Event")]

        # Working
        interface = HDTInterface()
        try:
            NodeExpansion(rdf_type=rdf_type,
                            args_filtering=ARGS_FILTERING,
                            interface=interface)
        except ValueError as error:
            print(error)
            self.fail("NodeExpansion()raised ValueError unexpectedly")

        # Not working
        with self.assertRaises(AttributeError):
            NodeExpansion(rdf_type=rdf_type,
                          args_filtering=ARGS_FILTERING,
                          interface="test")

    def test_filter_sub_graph(self):
        """ Test get_output_triples """
        folder = os.path.join(FOLDER_PATH, "src/tests/data/")
        ingoing_expected = clean_df(pd.read_csv(f"{folder}hdt_ingoing_expected.csv"))
        outgoing_expected = clean_df(pd.read_csv(f"{folder}hdt_outgoing_expected.csv"))
        types_expected = clean_df(pd.read_csv(f"{folder}hdt_types_expected.csv"))

        rdf_type =  [("event", "http://dbpedia.org/ontology/Event")]

        expansion = NodeExpansion(rdf_type=rdf_type,
                                  args_filtering=ARGS_FILTERING,
                                  interface=HDTInterface())

        to_discard = expansion.filtering(
            ingoing=ingoing_expected, outgoing=outgoing_expected,
            type_date=types_expected, dates=["1789-01-01", "1804-12-31"])

        filtered = [k for k, sup_class in expansion.superclasses.items() \
                if any(elt in sup_class for elt in expansion.mapping.keys())] + \
                    list(expansion.mapping.keys())
        # Filter on types of nodes that should be retrieved
        to_keep = list(types_expected[(~types_expected.subject.isin(to_discard)) & \
            (types_expected.object.isin(filtered))].subject.unique())

        subgraph_ingoing_expected = ingoing_expected[ingoing_expected.subject.isin(to_keep)]
        path_ingoing_expected = ingoing_expected[~ingoing_expected.subject.isin(to_discard)]
        subgraph_outgoing_expected = outgoing_expected[outgoing_expected.object.isin(to_keep)]
        path_outgoing_expected = outgoing_expected[~outgoing_expected.object.isin(to_discard)]


        subgraph_ingoing, path_ingoing, subgraph_outgoing, path_outgoing, _ = \
            expansion.filter_sub_graph(
                types_expected, ingoing_expected,
                outgoing_expected, ["1789-01-01", "1804-12-31"])

        for df1, df2 in [(subgraph_ingoing, subgraph_ingoing_expected),
                           (path_ingoing, path_ingoing_expected),
                           (subgraph_outgoing, subgraph_outgoing_expected),
                           (path_outgoing, path_outgoing_expected)]:
            merged = df1.merge(
                df2, how='left',
                on=["subject", "object", "predicate", "regex_helper"])
            self.assertTrue(merged.shape == df1.shape)
            self.assertTrue(merged.shape == df2.shape)
