"""
#TO DO: add documentation on this script
"""
# TO DO heuristics: add other heuristics
# TO DO heuristics: when to call this ranker
from math import log

class Ranker:
    """
    #TO DO: add documentation on this script
    """
    def __init__(self, type_ranking="entropy_predicate"):
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

    # def _split(self, d):
    #     return

    def _sort_dict(self, dico, reverse):
        return list({k: v for k, v in sorted(dico.items(),
            key=lambda item: item[1], reverse=reverse)}.items())[0][0]

    def _add_entropy_score(self, dico):
        new_dico = dict()
        tot = sum(nb for _, nb in dico.items())
        for path, count in dico.items():
            new_dico[path] = - (count / tot) * log(count / tot)
        return new_dico

    def __call__(self, occurences):
        if "pred" in self.type:
            if "inverse" in self.type:
                return self._sort_dict(dico=occurences, reverse=False)
            if "entropy" in self.type:
                dico = self._add_entropy_score(dico=occurences)
                print(dico)
                return self._sort_dict(dico=dico, reverse=True)
            return self._sort_dict(dico=occurences, reverse=True)

        raise ValueError("Not implemented")


if __name__ == '__main__':
    ranker = Ranker(type_ranking="entropy_predicate")

    import pandas as pd
    args = dict(df=pd.read_csv("pending.csv"),
                path=["http://dbpedia.org/resource/Category:French_Revolution"])
    output = ranker(args)
    print(output)
