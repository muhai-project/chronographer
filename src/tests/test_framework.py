"""
Unittest of file `framework.py`, class GraphSearchFramework
python -m unittest -v test_framework.py

Different types of ranking strategies:
- pred_freq:
- entropy_pred_freq:
- inverse_pred_freq:
- pred_object_freq:
- entropy_pred_object_freq:
- inverse_pred_object_freq:
"""

import os
import unittest
from collections import defaultdict

import pandas as pd
from rdflib.term import URIRef
from settings import FOLDER_PATH
from src.framework import GraphSearchFramework

class TestGraphSearchFramework(unittest.TestCase):
    """ Test class for GraphSearchFramework class """

    def test_update_occurence_iter_1(self):
        """ Test update_occurence """
        config = {
            "rdf_type": [("event", URIRef("http://dbpedia.org/ontology/Event")),],
            "predicate_filter": [],
            "start": "http://dbpedia.org/resource/Category:French_Revolution",
            "iterations": 1,
            "type_interface": "triply",
        }

        pending_iter_1 = pd.read_csv(os.path.join(FOLDER_PATH,
                                                  "src/tests/triply_expected.csv"))
        pending_iter_1 = pending_iter_1[\
            pending_iter_1.predicate != "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"]

        occurences_expected_pred = {
                "http://purl.org/dc/terms/subject": 87,
                "http://www.w3.org/2004/02/skos/core#broader": 15
            }

        occurences_expected_pred_obj = {
            "http://purl.org/dc/terms/subject" \
                + ";http://dbpedia.org/resource/Category:French_Revolution": 87,
            "http://www.w3.org/2004/02/skos/core#broader" \
                + ";http://dbpedia.org/resource/Category:French_Revolution": 15
        }

        for type_ranking in ['pred_freq', 'entropy_pred_freq', 'inverse_pred_freq',
            'pred_object_freq', 'entropy_pred_object_freq', 'inverse_pred_object_freq']:

            framework = GraphSearchFramework(config=dict(**config,
                                                         **dict(type_ranking=type_ranking)))

            occurences_output = framework.update_occurence(dataframe=pending_iter_1,
                                                           occurence=defaultdict(int))
            output = set(occurences_output)

            if "pred_object" in type_ranking:
                self.assertTrue(set(occurences_expected_pred_obj.keys()) == output and \
                all(nb == occurences_output[elt] \
                    for elt, nb in occurences_expected_pred_obj.items()))
            else:
                self.assertTrue(set(occurences_expected_pred.keys()) == output and \
                all(nb == occurences_output[elt] \
                    for elt, nb in occurences_expected_pred.items()))

    def test_select_nodes_to_expand_iter_1(self):
        config = {
            "rdf_type": [("event", URIRef("http://dbpedia.org/ontology/Event")),],
            "predicate_filter": ["http://dbpedia.org/ontology/wikiPageWikiLink",
                                "http://dbpedia.org/ontology/wikiPageRedirects"],
            "start": "http://dbpedia.org/resource/Category:French_Revolution",
            "iterations": 1,
            "type_ranking": "entropy_pred_object_freq",
            "type_interface": "triply",
        }
        framework = GraphSearchFramework(config)
        to_expand = "http://www.w3.org/2004/02/skos/core#broader"

        triply_df = pd.read_csv(os.path.join(FOLDER_PATH, "src/tests/triply_expected.csv"))
        pending = triply_df[\
            triply_df.predicate != "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"]
        expected_output = set(pending[\
            pending.predicate == to_expand] \
            .subject.values)
        
        framework.to_expand = to_expand
        framework.pending_nodes = pending
        nodes, path = framework.select_nodes_to_expand()
        self.assertEqual(expected_output, set(nodes))
        self.assertEqual([to_expand], path)
