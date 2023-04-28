# -*- coding: utf-8 -*-
"""
Ranking paths depending on the chosen metric, and returning the highest scored one
"""
from math import log

class Ranker:
    """
    Main class for ranking
    """
    def __init__(self, type_ranking: str ="entropy_pred_freq",
                 low_thresold: int = 1, high_threshold: int = 200):
        """
        - `type_ranking`: type of ranking below, see below for options
        - `low_threshold`: minimal sample size for path
        - `high_threshold`: maximum sample size for path

        Type of ranking strategies implemented:
            - `pred_freq`:
            - `entropy_pred_freq`:
            - `inverse_pred_freq`:
            - `pred_object_freq`:
            - `entropy_pred_object_freq`:
            - `inverse_pred_object_freq`:
        """
        self.type = type_ranking
        self.low_thresold = low_thresold
        self.high_threshold = high_threshold


    @staticmethod
    def filter_dict(dico: str) -> str:
        """ Ordering dico based on path info
        If starts by 1: highest priority, then descending order """
        for order in ["1", "2", "3"]:
            if any(k.startswith(order) for k in dico.keys()):
                return {k: v for k, v in dico.items() if k.startswith(order)}
        return dico

    def _sort_dict(self, dico: str, reverse, filter_items: bool =True) -> (str, str):
        dico = self.filter_dict(dico=dico)
        sorted_filtered_items = []
        if filter_items:
            sorted_filtered_items = \
                list({k: v for k, v in sorted(dico.items(),
                    key=lambda item: item[1], reverse=reverse) if
                        self.low_thresold < v < self.high_threshold}.items())
        if not (filter_items and sorted_filtered_items):
            sorted_filtered_items = \
                list({k: v for k, v in sorted(dico.items(),
                    key=lambda item: item[1], reverse=reverse)}.items())

        if sorted_filtered_items:
            return sorted_filtered_items[0][0], sorted_filtered_items[0][1]
        return None, None


    @staticmethod
    def _add_entropy_score(dico: str) -> str:
        new_dico = {}
        tot = sum(nb for _, nb in dico.items())
        for path, count in dico.items():
            new_dico[path] = - (count / tot) * log(count / tot)
        return new_dico

    def __call__(self, occurences: dict) -> dict:
        """
        sorted values with superclass info
        1. domain/range + score
        2. manually selected predicates + score (N/A)
        3. Only score
        """
        if "pred" in self.type:
            if "inverse" in self.type:
                return self._sort_dict(dico=occurences, reverse=False)
            if "entropy" in self.type:
                dico = self._add_entropy_score(dico=occurences)
                return self._sort_dict(dico=dico, reverse=True,
                                       filter_items=False)
            return self._sort_dict(dico=occurences, reverse=True)

        raise ValueError("Not implemented")


if __name__ == '__main__':
    import os
    import json
    from settings import FOLDER_PATH

    ranker = Ranker(type_ranking="entropy_pred_freq")
    with open(os.path.join(
        FOLDER_PATH, "sample-data",
        "French_Revolution_occurences.json"), 'r', encoding='utf-8') as openfile:
        OCCURRENCES = json.load(openfile)
    output = ranker(occurences=OCCURRENCES)
    print(output)
