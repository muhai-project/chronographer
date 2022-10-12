# -*- coding: utf-8 -*-
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

import pandas as pd
from settings import FOLDER_PATH
from src.framework import GraphSearchFramework

def get_occ_pred(type_ranking, ingoing, outgoing):
    """ Get expected occurences """
    if "object" not in type_ranking:
        occ = ingoing.groupby("predicate") \
                .agg({"subject": "count"}).to_dict()['subject']
        occ.update(outgoing.groupby("predicate") \
                .agg({"subject": "count"}).to_dict()['subject'])

    else:
        occ = {f"{k};http://dbpedia.org/resource/French_Revolution": v \
            for k, v in ingoing.groupby("predicate") \
                .agg({"subject": "count"}).to_dict()['subject'].items()}
        occ.update({f"http://dbpedia.org/resource/French_Revolution;{k}": v \
            for k, v in outgoing.groupby("predicate") \
                .agg({"subject": "count"}).to_dict()['subject'].items()})

    return occ


class TestGraphSearchFramework(unittest.TestCase):
    """ Test class for GraphSearchFramework class """

    # def test_update_occurence_iter_1(self):
    #     """ Test update_occurence """
        # config = {
        #     "rdf_type": [("event", "http://dbpedia.org/ontology/Event"),],
        #     "predicate_filter": [],
        #     "start": "http://dbpedia.org/resource/Category:French_Revolution",
        #     "iterations": 1,
        #     "start_date": "1765-01-01",
        #     "end_date": "1783-12-31",
        #     "type_interface": "hdt",
        #     "type_metrics": ["precision", "recall", "f1"],
        #     "gold_standard": os.path.join(
        #         FOLDER_PATH, "sample-data", "French_Revolution_gs_events.csv"),
        #     "referents": os.path.join(
        #         FOLDER_PATH, "sample-data", "French_Revolution_referents.json"),
        #     "name_exp": "French_Revolution",
        #     "dataset_type": "dbpedia",
        #     "dataset_path": os.path.join(
        #         FOLDER_PATH, "dbpedia-snapshot-2021-09"),
        # }

    #     folder = os.path.join(FOLDER_PATH, "src/tests/data")
    #     pending_ingoing_iter_1 = pd.read_csv(
    #         os.path.join(folder, "hdt_ingoing_superclass_expected.csv")).fillna("")
    #     pending_outgoing_iter_1 = pd.read_csv(
    #         os.path.join(folder, "hdt_outgoing_superclass_expected.csv")).fillna("")

    #     for type_ranking in ['pred_freq', 'entropy_pred_freq', 'inverse_pred_freq',
    #         'pred_object_freq', 'entropy_pred_object_freq', 'inverse_pred_object_freq']:

    #         time.sleep(1)
    #         framework = GraphSearchFramework(config=dict(**config,
    #                                                      **dict(type_ranking=type_ranking)))
    #         print(f"TYPE RANKING TESTED: {framework.type_ranking}")

    #         occurences_output = framework.update_occurence(ingoing=pending_ingoing_iter_1,
    #                                                        outgoing=pending_outgoing_iter_1,
    #                                                        occurence=defaultdict(int))

    #         def f_process(x_input):
    #             return x_input[2:].replace("outgoing-", "").replace("ingoing-", "")

    #         occurences_output = {f_process(k): v \
    #             for k, v in occurences_output.items()}

    #         occurences_expected_pred = get_occ_pred(type_ranking,
    #                                                 pending_ingoing_iter_1,
    #                                                 pending_outgoing_iter_1)

    #         self.assertTrue(occurences_expected_pred == occurences_output)

    def test_select_nodes_to_expand_iter_1(self):
        """ Test selecting next nodes to expand """
        config = {
            "rdf_type": [("event", "http://dbpedia.org/ontology/Event"),],
            "predicate_filter": [],
            "start": "http://dbpedia.org/resource/Category:French_Revolution",
            "iterations": 1,
            "start_date": "1765-01-01",
            "end_date": "1783-12-31",
            "type_interface": "hdt",
            "type_metrics": ["precision", "recall", "f1"],
            "gold_standard": os.path.join(
                FOLDER_PATH, "sample-data", "French_Revolution_gs_events.csv"),
            "referents": os.path.join(
                FOLDER_PATH, "sample-data", "French_Revolution_referents.json"),
            "name_exp": "French_Revolution",
            "dataset_type": "dbpedia",
            "dataset_path": os.path.join(
                FOLDER_PATH, "dbpedia-snapshot-2021-09"),
        }

        to_expand_all = {
            'pred_freq': 'http://dbpedia.org/ontology/isPartOfMilitaryConflict',
            "pred_object_freq": 'ingoing-http://dbpedia.org/ontology/isPartOfMilitaryConflict;' + \
                'http://dbpedia.org/resource/French_Revolution',
            'entropy_pred_freq':
                'http://dbpedia.org/ontology/isPartOfMilitaryConflict',
            'entropy_pred_object_freq':
                'ingoing-http://dbpedia.org/ontology/isPartOfMilitaryConflict;' + \
                'http://dbpedia.org/resource/French_Revolution',
            'inverse_pred_freq': 'http://dbpedia.org/property/events',
            'inverse_pred_object_freq': 'ingoing-http://dbpedia.org/property/events;' + \
                'http://dbpedia.org/resource/French_Revolution'
        }

        folder = os.path.join(FOLDER_PATH, "src/tests/data")
        pending_ingoing_iter_1 = pd.read_csv(
            os.path.join(folder, "hdt_ingoing_superclass_expected.csv")) \
                .fillna("")[["subject", "object", "predicate"]]
        pending_outgoing_iter_1 = pd.read_csv(
            os.path.join(folder, "hdt_outgoing_superclass_expected.csv")) \
                .fillna("")[["subject", "object", "predicate"]]

        military_conflicts = {'http://dbpedia.org/resource/Battle_of_Kaiserslautern',
                              'http://dbpedia.org/resource/Fall_of_Maximilien_Robespierre',
                              'http://dbpedia.org/resource/Insurrection_of_31_May_–_2_June_1793',
                              'http://dbpedia.org/resource/Battle_of_Wattignies',
                              'http://dbpedia.org/resource/Siege_of_Maubeuge_(1793)',
                              'http://dbpedia.org/resource/Day_of_Daggers',
                              'http://dbpedia.org/resource/Coup_of_18_Fructidor',
                              'http://dbpedia.org/resource/Revolt_of_1_Prairial_Year_III',
                              'http://dbpedia.org/resource/13_Vendémiaire',
                              'http://dbpedia.org/resource/Insurrection_of_10_August_1792',
                              'http://dbpedia.org/resource/Storming_of_the_Bastille',
                              'http://dbpedia.org/resource/Insurrection_of_12_Germinal,_Year_III',
                              'http://dbpedia.org/resource/Demonstration_of_20_June_1792'}

        events = {'http://dbpedia.org/resource/Bastille',
                  'http://dbpedia.org/resource/Square_du_Temple'}

        expected_outputs = {
            'pred_freq':  military_conflicts,
            "pred_object_freq": military_conflicts,
            'entropy_pred_freq': military_conflicts,
            'entropy_pred_object_freq': military_conflicts,
            'inverse_pred_freq': events,
            'inverse_pred_object_freq': events,
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
            print(f"TYPE RANKING: {type_ranking}")

            self.assertEqual(expected_output, set(nodes))
            self.assertEqual([to_expand], path)
