import numpy as np 

# TO DO heuristics: add other heuristics
# TO DO heuristics: when to call this ranker

class Ranker:

    def __init__(self, type_ranking="entropy_predicate"):
        """
        Type of ranking strategies implemented:
            - pred_freq:
            - inverse_pred_freq: 
            - pred_object_freq: 
            - inverse_pred_object_freq: 
            - subject_freq: 
            - inverse_subject_freq: 
            - inverse_pred_object_split_freq: 
        """
        self.type = type_ranking
    
    def _split(self, d):
        return
    
    def _sort_dict(self, d, reverse):
        return list({k: v for k, v in sorted(d.items(), key=lambda item: item[1], reverse=reverse)}.items())[0]
    
    def __call__(self, occurences):
        if "pred" in self.type:
            if "inverse" in self.type:
                return self._sort_dict(d=occurences, reverse=False)
            else:
                return self._sort_dict(d=occurences, reverse=True)
        else:
            raise ValueError("Not implemented")


if __name__ == '__main__':
    ranker = Ranker(type_ranking="entropy_predicate")

    import pandas as pd 
    args = dict(df=pd.read_csv("pending.csv"), path=["http://dbpedia.org/resource/Category:French_Revolution"])
    output = ranker(args)
    print(output)
