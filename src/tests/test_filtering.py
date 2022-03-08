"""
Unittest of file `filtering.py`, class Filtering
python -m unittest -v test_filtering.py
"""

import unittest
import pandas as pd
from src.filtering import Filtering

class TestFiltering(unittest.TestCase):
    """
    Test class for Filtering class
    """
    def test_get_to_discard_date(self):
        """ Test get_to_discard_date """
        dates = ["1789-01-01", "1804-12-31"]
        filtering = Filtering(args=dict(where=1, when=1))

        df_pd = pd.DataFrame({
            "predicate": \
                ["http://dbpedia.org/ontology/date"] * 5 + \
                    ["http://dbpedia.org/ontology/startDate"] * 3 + \
                        ["http://dbpedia.org/ontology/endDate"] * 3 + \
                            ["http://dbpedia.org/property/birthDate"] * 3 + \
                                ["http://dbpedia.org/property/deathDate"] * 3 + \
                                    ["test_predicate_1", "test_predicate_2"],
            "object": \
                ["1795-03-08", "1788-01-01", "1815-12-31", "1789-01-01", "1804-12-31"] + \
                    ["1795-03-08", "1815-12-31", "1804-12-31"] + \
                        ["1795-03-08", "1788-01-01", "1789-01-01"] + \
                            ["1795-03-08", "1815-12-31", "1804-12-31"] + \
                                ["1795-03-08", "1788-01-01", "1789-01-01"] + \
                                    ["1788-01-01", "1815-12-31"],
            "subject": \
                [f"date{i}" for i in range(1, 6)] + \
                    [f"startDate{i}" for i in range(1, 4)] + \
                        [f"endDate{i}" for i in range(1, 4)] + \
                            [f"birthDate{i}" for i in range(1, 4)] + \
                                [f"deathDate{i}" for i in range(1, 4)] + \
                                    ["random1", "random2"]
        })

        discarded = set(["date2", "date3", "startDate2", "endDate2", "birthDate2", "deathDate2"])

        to_discard = filtering.get_to_discard_date(date_df=df_pd, dates=dates)
        self.assertTrue(discarded == set(to_discard))


    def test_get_to_discard_regex(self):
        """ Test get_to_discard_regex """
        dates = ["1789-01-01", "1804-12-31"]
        filtering = Filtering(args=dict(where=1, when=1))

        df_pd = pd.DataFrame({
            "subject": [
                "1999_legendary",
                "1795_legendary",
                "1851_legendary",
                "legendary",
            ]
        })

        discarded = set(["1999_legendary", "1851_legendary"])
        to_discard = filtering.get_to_discard_regex(df_pd=df_pd, dates=dates)
        self.assertTrue(discarded == set(to_discard))


    def test_get_to_discard_location(self):
        """ Test get_to_discard_location """
        filtering = Filtering(args=dict(where=1, when=1))

        df_pd = pd.DataFrame({
            "subject": [
                f"subject{i}" for i in range(1, 5)
            ],
            "object": [
                "http://dbpedia.org/ontology/Place",
                "http://dbpedia.org/ontology/Location",
                "hello", "world"
            ]
        })

        discarded = set(["subject1", "subject2"])
        to_discard = filtering.get_to_discard_location(df_pd=df_pd)
        self.assertTrue(discarded == set(to_discard))
