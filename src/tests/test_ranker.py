# -*- coding: utf-8 -*-
"""
Unittest of file `framework.py`, class GraphSearchFramework
python -m unittest -v test_ranker.py

Different types of ranking strategies:
- pred_freq:
- entropy_pred_freq:
- inverse_pred_freq:
- pred_object_freq:
- entropy_pred_object_freq:
- inverse_pred_object_freq:
"""

import unittest
from src.ranker import Ranker

class TestRanker(unittest.TestCase):
    """ Test class for Ranker """
    def test_call_pred_freq(self):
        """ type_ranking='pred_freq' """
        ranker = Ranker(type_ranking='pred_freq')

        occurences_expected = {
            "http://purl.org/dc/terms/subject": 87,
            "http://www.w3.org/2004/02/skos/core#broader": 15
        }

        self.assertEqual(ranker(occurences_expected)[0],
                            "http://purl.org/dc/terms/subject")

    def test_call_entropy_pred_freq(self):
        """ type_ranking='entropy_pred_freq' """
        ranker = Ranker(type_ranking='entropy_pred_freq')

        occurences_expected = {
            "http://purl.org/dc/terms/subject": 87,
            "http://www.w3.org/2004/02/skos/core#broader": 15
        }

        self.assertEqual(ranker(occurences_expected)[0],
            "http://www.w3.org/2004/02/skos/core#broader")

    def test_call_inverse_pred_freq(self):
        """ type_ranking='inverse_pred_freq' """
        ranker = Ranker(type_ranking='inverse_pred_freq')

        occurences_expected = {
            "http://purl.org/dc/terms/subject": 87,
            "http://www.w3.org/2004/02/skos/core#broader": 15
        }

        self.assertEqual(ranker(occurences_expected)[0],
                          "http://www.w3.org/2004/02/skos/core#broader")

    def test_call_pred_object_freq(self):
        """ type_ranking='pred_freq' """
        ranker = Ranker(type_ranking='pred_object_freq')

        occurences_expected = {
            "http://purl.org/dc/terms/subject": 87,
            "http://www.w3.org/2004/02/skos/core#broader": 15
        }

        self.assertEqual(ranker(occurences_expected)[0],
                            "http://purl.org/dc/terms/subject")

    def test_call_entropy_pred_object_freq(self):
        """ type_ranking='entropy_pred_object_freq' """
        ranker = Ranker(type_ranking='entropy_pred_object_freq')

        occurences_expected = {
            "http://purl.org/dc/terms/subject": 87,
            "http://www.w3.org/2004/02/skos/core#broader": 15
        }

        self.assertEqual(ranker(occurences_expected)[0],
            "http://www.w3.org/2004/02/skos/core#broader")

    def test_call_inverse_pred_object_freq(self):
        """ type_ranking='inverse_pred_object_freq' """
        ranker = Ranker(type_ranking='inverse_pred_object_freq')

        occurences_expected = {
            "http://purl.org/dc/terms/subject": 87,
            "http://www.w3.org/2004/02/skos/core#broader": 15
        }

        self.assertEqual(ranker(occurences_expected)[0],
                          "http://www.w3.org/2004/02/skos/core#broader")
