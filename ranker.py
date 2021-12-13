import numpy as np 

# TO DO heuristics: add other heuristics
# TO DO heuristics: when to call this ranker

class Ranker:

    def __init__(self, type_ranking="entropy_predicate"):
        self.type = type_ranking
    
    def _groupby_pred(self, df):
        return df.groupby("predicate").agg({"subject": "nunique"})
    
    def _get_ranked(self, df, col_score, path):
        df = df.sort_values(by=col_score, ascending=False)
        return [([str(elt) for elt in path] + [str(index)], row[col_score]) for index, row in df.iterrows()]

    def _call_freq_predicate(self, args):
        return self._get_ranked(df=self._groupby_pred(df=args["df"]),
                                col_score="subject",
                                path=args["path"])
    
    def _call_entropy_predicate(self, args):
        grouped = self._groupby_pred(df=args["df"])
        tot = grouped.subject.values.sum()
        grouped["prob"] = grouped["subject"] / tot
        grouped["entropy"] = - grouped["prob"] * np.log(grouped["prob"])
        
        return self._get_ranked(df=grouped,
                                col_score="entropy",
                                path=args["path"])
    
    def __call__(self, args):
        if self.type == "frequency_predicate":
            return self._call_freq_predicate(args)
        
        if self.type == "entropy_predicate":
            return self._call_entropy_predicate(args)
        
        else:
            raise ValueError("Not implemented")


if __name__ == '__main__':
    ranker = Ranker(type_ranking="entropy_predicate")

    import pandas as pd 
    args = dict(df=pd.read_csv("pending.csv"), path=["http://dbpedia.org/resource/Category:French_Revolution"])
    output = ranker(args)
    print(output)
