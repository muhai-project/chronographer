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

def get_occ_pred_plus(df_pd, groupby_list, count):
    """ Grouping df by at least two arguments
    Returning correctly formatted dictionnary (keys=grouped, val=count) """
    grouped = df_pd.groupby(groupby_list) \
        .agg({count: "count"}).to_dict()[count]
    return {";".join(k): v for k, v in grouped.items()}

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
            "type_metrics": ["precision", "recall", "f1"],
            "gold_standard": "events.csv",
        }

        folder = os.path.join(FOLDER_PATH, "src/tests")
        pending_ingoing_iter_1 = pd.read_csv(
            os.path.join(folder, "triply_ingoing_expected.csv")).fillna("")
        pending_outgoing_iter_1 = pd.read_csv(
            os.path.join(folder, "triply_outgoing_expected.csv")).fillna("")

        occurences_expected_pred = pd.concat([pending_ingoing_iter_1, pending_outgoing_iter_1]) \
            .groupby("predicate").agg({"subject": "count"}).to_dict()['subject']

        occurences_expected_pred_obj = get_occ_pred_plus(
            df_pd=pending_ingoing_iter_1, groupby_list=["predicate", "object"], count="subject")

        occurences_expected_pred_obj.update(
            get_occ_pred_plus(df_pd=pending_outgoing_iter_1,
                              groupby_list=["subject", "predicate"], count="object")
        )

        for type_ranking in ['pred_freq', 'entropy_pred_freq', 'inverse_pred_freq',
            'pred_object_freq', 'entropy_pred_object_freq', 'inverse_pred_object_freq']:

            framework = GraphSearchFramework(config=dict(**config,
                                                         **dict(type_ranking=type_ranking)))

            occurences_output = framework.update_occurence(ingoing=pending_ingoing_iter_1,
                                                           outgoing=pending_outgoing_iter_1,
                                                           occurence=defaultdict(int))
            occurences_output = {k.replace("ingoing-", "").replace("outgoing-", ""): v \
                for k, v in occurences_output.items()}
            output = set(occurences_output)

            if "pred_object" in type_ranking:
                self.assertTrue(set(occurences_expected_pred_obj) == output and \
                all(nb == occurences_output[elt] \
                    for elt, nb in occurences_expected_pred_obj.items()))
            else:
                self.assertTrue(set(occurences_expected_pred) == output and \
                all(nb == occurences_output[elt] \
                for elt, nb in occurences_expected_pred.items()))

    def test_select_nodes_to_expand_iter_1(self):
        """ Test selecting next nodes to expand """
        config = {
            "rdf_type": [("event", URIRef("http://dbpedia.org/ontology/Event")),],
            "predicate_filter": ["http://dbpedia.org/ontology/wikiPageWikiLink",
                                "http://dbpedia.org/ontology/wikiPageRedirects"],
            "start": "http://dbpedia.org/resource/Category:French_Revolution",
            "iterations": 1,
            "type_ranking": "entropy_pred_object_freq",
            "type_interface": "triply",
            "type_metrics": ["precision", "recall", "f1"],
            "gold_standard": "events.csv",
        }

        to_expand_all = {
            'pred_freq': 'http://www.w3.org/2000/01/rdf-schema#label',
            "pred_object_freq": 'outgoing-http://dbpedia.org/resource/French_Revolution;' + \
                'http://www.w3.org/2000/01/rdf-schema#label',
            'entropy_pred_freq': 'http://www.w3.org/2000/01/rdf-schema#label',
            'entropy_pred_object_freq': 'outgoing-http://dbpedia.org/resource/' + \
                'French_Revolution;http://www.w3.org/2000/01/rdf-schema#label',
            'inverse_pred_freq': 'http://dbpedia.org/property/events',
            'inverse_pred_object_freq': 'ingoing-http://dbpedia.org/property/events;' + \
                'http://dbpedia.org/resource/French_Revolution'
        }

        folder = os.path.join(FOLDER_PATH, "src/tests")
        pending_ingoing_iter_1 = pd.read_csv(
            os.path.join(folder, "triply_ingoing_expected.csv")) \
                .fillna("")[["subject", "object", "predicate"]]
        pending_outgoing_iter_1 = pd.read_csv(
            os.path.join(folder, "triply_outgoing_expected.csv")) \
                .fillna("")[["subject", "object", "predicate"]]

        expected_outputs = {
            'pred_freq':  set(pending_ingoing_iter_1[\
                    pending_ingoing_iter_1.predicate == \
                        'http://www.w3.org/2000/01/rdf-schema#label'] \
                    .subject.values) \
                        .union(set(pending_outgoing_iter_1[\
                    pending_outgoing_iter_1.predicate == \
                        'http://www.w3.org/2000/01/rdf-schema#label']\
                        .object.values)),
            "pred_object_freq": set(pending_outgoing_iter_1[
                        (pending_outgoing_iter_1.predicate == \
                            'http://www.w3.org/2000/01/rdf-schema#label') & \
                        (pending_outgoing_iter_1.subject == \
                            'http://dbpedia.org/resource/French_Revolution')] \
                                .object.values),
            'entropy_pred_freq': set(pending_ingoing_iter_1[\
                    pending_ingoing_iter_1.predicate == \
                        'http://www.w3.org/2000/01/rdf-schema#label'] \
                        .subject.values) \
                        .union(set(pending_outgoing_iter_1[\
                    pending_outgoing_iter_1.predicate == \
                        'http://www.w3.org/2000/01/rdf-schema#label'] \
                        .object.values)),
            'entropy_pred_object_freq': set(pending_outgoing_iter_1[
                        (pending_outgoing_iter_1.predicate == \
                            'http://www.w3.org/2000/01/rdf-schema#label') & \
                        (pending_outgoing_iter_1.subject == \
                            'http://dbpedia.org/resource/French_Revolution')] \
                                .object.values),
            'inverse_pred_freq': set(pending_ingoing_iter_1[\
                    pending_ingoing_iter_1.predicate == \
                        'http://dbpedia.org/property/events'] \
                            .subject.values) \
                        .union(set(pending_outgoing_iter_1[\
                    pending_outgoing_iter_1.predicate == \
                        'http://dbpedia.org/property/events'] \
                            .object.values)),
            'inverse_pred_object_freq': set(pending_ingoing_iter_1[
                        (pending_ingoing_iter_1.predicate == \
                            'http://dbpedia.org/property/events') & \
                        (pending_ingoing_iter_1.object == \
                            'http://dbpedia.org/resource/French_Revolution')] \
                                .subject.values),
        }

        for type_ranking in ['pred_freq', 'entropy_pred_freq', 'inverse_pred_freq',
            'pred_object_freq', 'entropy_pred_object_freq', 'inverse_pred_object_freq']:
            config["type_ranking"] = type_ranking
            framework = GraphSearchFramework(config)

            to_expand = to_expand_all[type_ranking]
            expected_output = expected_outputs[type_ranking]
            framework.to_expand = to_expand
            framework.pending_nodes_ingoing = pending_ingoing_iter_1
            framework.pending_nodes_outgoing = pending_outgoing_iter_1

            nodes, path = framework.select_nodes_to_expand()
            self.assertEqual(expected_output, set(nodes))
            self.assertEqual([to_expand], path)
