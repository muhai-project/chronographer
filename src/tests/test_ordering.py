"""
Unittest of file `framework.py`, class GraphSearchFramework
python -m unittest -v test_ordering.py
"""

import os
import unittest
import pandas as pd

from settings import FOLDER_PATH
from src.ordering import Ordering
from src.triply_interface import TriplInterface

def read_csv(path_df, cols):
    """ Read csv + simple preprocessing steps """
    return pd.read_csv(path_df).fillna("")[cols]

INTERFACE = TriplInterface()

class TestOrdering(unittest.TestCase):
    """ Test class for Ordering """

    def test_add_superclass(self):
        """ Test add_superclass_to_class function """
        ordering = Ordering(interface=INTERFACE)

        folder = os.path.join(FOLDER_PATH, "src/tests/data")
        ingoing = read_csv(
            os.path.join(folder, "triply_ingoing_expected.csv"),
            ["subject", "predicate", "object"])
        ingoing = ordering.remove_literals(triple_df=ingoing)

        domain_expected = {
            'http://dbpedia.org/ontology/internationalAffiliation': \
                'http://dbpedia.org/ontology/PoliticalParty',
            'http://dbpedia.org/ontology/deathPlace': \
                'http://dbpedia.org/ontology/Animal',
            'http://dbpedia.org/ontology/isPartOfMilitaryConflict': \
                'http://dbpedia.org/ontology/MilitaryConflict',
            'http://dbpedia.org/ontology/nonFictionSubject': \
                'http://dbpedia.org/ontology/WrittenWork',
        }

        superclasses_expected = {
            'http://dbpedia.org/ontology/PoliticalParty': \
                'http://dbpedia.org/ontology/Agent',
            'http://dbpedia.org/ontology/Animal': \
                'http://dbpedia.org/ontology/Species',
            'http://dbpedia.org/ontology/MilitaryConflict': \
                'http://dbpedia.org/ontology/Event',
            'http://dbpedia.org/ontology/WrittenWork': \
                'http://dbpedia.org/ontology/Work',
        }

        ordering.add_superclass_to_class(df_pd=ingoing, type_node='ingoing')
        self.assertTrue(ordering.domain == domain_expected)
        self.assertTrue(ordering.superclasses == superclasses_expected)

    def test_add_superclass_to_df(self):
        """ Test add_superclass_to_df function """
        ordering = Ordering(interface=INTERFACE,
                            focus_for_search=["http://dbpedia.org/ontology/Event"])

        folder = os.path.join(FOLDER_PATH, "src/tests/data")
        ingoing = read_csv(
            os.path.join(folder, "triply_ingoing_expected.csv"),
            ["subject", "predicate", "object"])
        ingoing = ordering.remove_literals(triple_df=ingoing)

        ingoing_superclass_expected = read_csv(
            os.path.join(folder, "triply_ingoing_superclass_expected.csv"),
            cols=["subject", "predicate", "object", "superclass"])

        domain = {
            'http://dbpedia.org/ontology/internationalAffiliation': \
                'http://dbpedia.org/ontology/PoliticalParty',
            'http://dbpedia.org/ontology/deathPlace': \
                'http://dbpedia.org/ontology/Animal',
            'http://dbpedia.org/ontology/isPartOfMilitaryConflict': \
                'http://dbpedia.org/ontology/MilitaryConflict',
            'http://dbpedia.org/ontology/nonFictionSubject': \
                'http://dbpedia.org/ontology/WrittenWork',
        }

        superclasses = {
            'http://dbpedia.org/ontology/PoliticalParty': \
                'http://dbpedia.org/ontology/Agent',
            'http://dbpedia.org/ontology/Animal': \
                'http://dbpedia.org/ontology/Species',
            'http://dbpedia.org/ontology/MilitaryConflict': \
                'http://dbpedia.org/ontology/Event',
            'http://dbpedia.org/ontology/WrittenWork': \
                'http://dbpedia.org/ontology/Work',
        }
        ordering.domain = domain
        ordering.superclasses = superclasses

        ingoing_superclass = ordering.add_superclass_to_df(triple_df=ingoing,
                                                            type_node="ingoing")
        merged = ingoing_superclass_expected.merge(
            ingoing_superclass, how='left', on=["subject", "predicate", "object", "superclass"])
        self.assertTrue(merged.shape == ingoing_superclass_expected.shape)
        self.assertTrue(merged.shape == ingoing_superclass.shape)

    def test_update_info_filter(self):
        """ Test update_info_filter function """
        ordering = Ordering(interface=INTERFACE)

        folder = os.path.join(FOLDER_PATH, "src/tests/data")

        ingoing_superclass = read_csv(
            os.path.join(folder, "triply_ingoing_superclass_expected.csv"),
            cols=["subject", "predicate", "object", "superclass"])
        ingoing_superclass_filtered_expected = read_csv(
            os.path.join(folder, "triply_ingoing_superclass_filtered_expected.csv"),
            cols=["subject", "predicate", "object", "superclass"])

        ingoing_superclass_filtered, info = ordering.update_info_filter(
            triple_df=ingoing_superclass, type_node="ingoing", info={}, iteration=1)

        ingoing_superclass_filtered.to_csv("filtered.csv")
        ingoing_superclass_filtered_expected.to_csv("filtered_expected.csv")
        merged = ingoing_superclass_filtered_expected.merge(
            ingoing_superclass_filtered, how='left',
            on=["subject", "predicate", "object", "superclass"])
        self.assertTrue(merged.shape == ingoing_superclass_filtered_expected.shape)
        self.assertTrue(merged.shape == ingoing_superclass_filtered.shape)

        info_expected = {
            1: {"ingoing": 64, "ingoing_domain": 20, "ingoing_domain_relevant": 14,
                "outgoing": 0, "outgoing_range": 0, "outgoing_range_relevant": 0,}
        }
        self.assertTrue(info == info_expected)
