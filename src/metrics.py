# -*- coding: utf-8 -*-
"""
Metrics for assessing the quality of the output
"""
import json

class Metrics:
    """
    Quantitative metrics: precision, recall, f1
    """

    def __init__(self, referent_path):
        self.metrics_to_calc = {
            'precision': self.get_precision,
            'recall': self.get_recall,
            'f1': self.get_f1,
        }

        with open(referent_path, "r", encoding='utf-8') as openfile:
            self.referents = json.load(openfile)

    @staticmethod
    def get_numbers(found, gold_standard):
        """ Numbers necessary to calculate the metrics """
        found, gold_standard = set(found), set(gold_standard)
        true_pos = len(found.intersection(gold_standard))
        false_pos = len(found.difference(gold_standard))
        false_neg = len(gold_standard.difference(found))
        return dict(true_pos=true_pos,
                    false_pos=false_pos,
                    false_neg=false_neg)

    @staticmethod
    def get_precision(**args):
        """ Precision """
        if args["true_pos"] + args["false_pos"] == 0:
            return 0
        return args["true_pos"] / (args["true_pos"] + args["false_pos"])

    @staticmethod
    def get_recall(**args):
        """ Recall """
        if args["true_pos"] + args["false_neg"] == 0:
            return 0
        return args["true_pos"] / (args["true_pos"] + args["false_neg"])

    @staticmethod
    def get_f1(**args):
        """ f1 """
        if args["true_pos"] + \
            0.5 * (args["false_pos"] + args["false_neg"]) == 0:
            return 0
        return args["true_pos"] / (args["true_pos"] + \
            0.5 * (args["false_pos"] + args["false_neg"]))

    def __call__(self, found: list, gold_standard: list,
                 type_metrics: list):
        def f_change(url):
            return self.referents[url] if url in self.referents else url
        found = [f_change(url) for url in found]

        if any(metric not in self.metrics_to_calc for metric in type_metrics):
            raise ValueError(f"Current metrics implemented: {list(self.metrics_to_calc.keys())}" \
                + "\tOne of the metrics in parameter not implemented")

        args= self.get_numbers(found=found, gold_standard=gold_standard)
        _metrics = {metric: f(**args) \
            for metric, f in self.metrics_to_calc.items()}
        return _metrics


if __name__ == '__main__':
    import os
    import pandas as pd
    from settings import FOLDER_PATH
    metrics = Metrics(referent_path=os.path.join(FOLDER_PATH, "sample-data", "French_Revolution_referents.json"))

    df_gs = pd.read_csv(os.path.join(FOLDER_PATH, "sample-data", "French_Revolution_gs_events.csv"))
    event_gs = list(df_gs[df_gs['linkDBpediaEn']!=''].linkDBpediaEn.unique())
    print("Event: French Revolution")
    print(f"# of sub-events: {len(event_gs)}")
    TYPE_METRICS=['precision', 'recall', 'f1']

    subgraph = pd.read_csv(os.path.join(
        FOLDER_PATH, "sample-data", "French_Revolution_subgraph.csv"))
    events_found = \
                [str(e) for e in subgraph[subgraph.type_df == "ingoing"] \
                    .subject.unique()] + \
                    [str(e) for e in subgraph[subgraph.type_df == "outgoing"] \
                        .object.unique()]
    res = metrics(found=events_found, gold_standard=event_gs, type_metrics=TYPE_METRICS)
    print(res)
