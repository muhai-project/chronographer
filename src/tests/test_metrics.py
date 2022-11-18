# -*- coding: utf-8 -*-
"""
Unittest of file `metrics.py`, class Metrics
python -m unittest -v test_metrics.py
"""
import os
import unittest
from src.metrics import Metrics
from settings import FOLDER_PATH

config_metrics = {
        "referents": os.path.join(
            FOLDER_PATH, "sample-data", "French_Revolution_referents.json"),
        "type_metrics": ['precision', 'recall', 'f1'],
        "gold_standard": os.path.join(
            FOLDER_PATH, "sample-data", "French_Revolution_gs_events.csv")
    }

class TestMetrics(unittest.TestCase):
    """
    Test class for Metrics class
    """

    def test_get_precision(self):
        """ Test get_precision """
        metrics = Metrics(config_metrics=config_metrics)
        args = {"true_pos": 90, "false_pos": 30}
        self.assertEqual(metrics.get_precision(**args), 0.75)

        args = {"true_pos": 0, "false_pos": 0}
        self.assertEqual(metrics.get_precision(**args), 0)

        args = {"true_pos": 45, "false_pos": 5}
        self.assertEqual(metrics.get_precision(**args), 0.9)

    def test_get_recall(self):
        """ Test get_recall """
        metrics = Metrics(config_metrics=config_metrics)

        args = {"true_pos": 90, "false_neg": 10}
        self.assertEqual(metrics.get_recall(**args), 0.9)

        args = {"true_pos": 0, "false_neg": 0}
        self.assertEqual(metrics.get_recall(**args), 0)

    def test_get_f1(self):
        """ Test get_f1 """
        metrics = Metrics(config_metrics=config_metrics)

        args = {"true_pos": 90, "false_neg": 0, "false_pos": 0}
        self.assertEqual(metrics.get_f1(**args), 1)

        args = {"true_pos": 95, "false_neg": 5, "false_pos": 55}
        self.assertEqual(metrics.get_f1(**args), 0.76)

    def test_get_numbers(self):
        """ Test get_numbers """
        metrics = Metrics(config_metrics=config_metrics)

        found = [1, 2, 3, 5, 7, 10]
        gold_standard = [3, 4, 7, 8, 9, 10]

        true_pos, false_pos, false_neg = 3, 3, 3
        numbers = metrics.get_numbers(found, gold_standard)

        self.assertEqual(true_pos, numbers['true_pos'])
        self.assertEqual(false_pos, numbers['false_pos'])
        self.assertEqual(false_neg, numbers['false_neg'])
