"""
#TO DO: add documentation on this script
"""
from math import log

class Ranker:
    """
    #TO DO: add documentation on this script
    """
    def __init__(self, type_ranking: str ="entropy_predicate",
                 low_thresold: int = 1, high_threshold: int = 200):
        """
        Type of ranking strategies implemented:
            - pred_freq:
            - entropy_pred_freq:
            - inverse_pred_freq:
            - pred_object_freq:
            - entropy_pred_object_freq:
            - inverse_pred_object_freq:
        """
        self.type = type_ranking
        self.low_thresold = low_thresold
        self.high_threshold = high_threshold

        self.domain = "http://www.w3.org/2000/01/rdf-schema#domain"
        self.range = "http://www.w3.org/2000/01/rdf-schema#range"

    # def _split(self, d):
    #     return

    @staticmethod
    def filter_dict(dico):
        """ Filtering dico based on path info
        If starts by 1: highest priority, then descending order """
        for order in ["1", "2", "3"]:
            if any(k.startswith(order) for k in dico.keys()):
                return {k: v for k, v in dico.items() if k.startswith(order)}
        return dico

    def _sort_dict(self, dico, reverse, filter_items=True):
        dico = self.filter_dict(dico=dico)
        if filter_items:
            sorted_filtered_items = \
                list({k: v for k, v in sorted(dico.items(),
                    key=lambda item: item[1], reverse=reverse) if
                        self.low_thresold < v < self.high_threshold}.items())
        else:
            sorted_filtered_items = \
                list({k: v for k, v in sorted(dico.items(),
                    key=lambda item: item[1], reverse=reverse)}.items())
        if sorted_filtered_items:
            return sorted_filtered_items[0][0]
        return None

    @staticmethod
    def _add_entropy_score(dico):
        new_dico = {}
        tot = sum(nb for _, nb in dico.items())
        for path, count in dico.items():
            new_dico[path] = - (count / tot) * log(count / tot)
        return new_dico

    def __call__(self, occurences):
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
    ranker = Ranker(type_ranking="entropy_predicate")

    import pandas as pd
    args = dict(df=pd.read_csv("pending.csv"),
                path=["http://dbpedia.org/resource/Category:French_Revolution"])
    output = ranker(args)
    print(output)
