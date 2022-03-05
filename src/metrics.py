"""
#TO DO: add documentation on this script
"""
import json

class Metrics:
    """
#TO DO: add documentation on this script
"""

    def __init__(self, referent_path):
        self.metrics_to_calc = {
            'precision': self._get_precision,
            'recall': self._get_recall,
            'f1': self._get_f1,
        }

        self.referents = json.load(open(referent_path, "r", encoding="utf-8"))

    def _get_numbers(self, found, gold_standard):
        found, gold_standard = set(found), set(gold_standard)
        true_pos = len(found.intersection(gold_standard))
        false_pos = len(found.difference(gold_standard))
        false_neg = len(gold_standard.difference(found))
        return dict(true_pos=true_pos,
                    false_pos=false_pos,
                    false_neg=false_neg)

    @staticmethod
    def _get_precision(**args):
        if args["true_pos"] + args["false_pos"] == 0:
            return 0
        return args["true_pos"] / (args["true_pos"] + args["false_pos"])

    @staticmethod
    def _get_recall(**args):
        if args["true_pos"] + args["false_neg"] == 0:
            return 0
        return args["true_pos"] / (args["true_pos"] + args["false_neg"])

    @staticmethod
    def _get_f1(**args):
        if args["true_pos"] + \
            0.5 * (args["false_pos"] + args["false_neg"]) == 0:
            return 0
        return args["true_pos"] / (args["true_pos"] + \
            0.5 * (args["false_pos"] + args["false_neg"]))

    def __call__(self, found: list, gold_standard: list,
                 type_metrics: list):
        f_change = lambda url: self.referents[url] if url in self.referents else url
        found = [f_change(url) for url in found]

        if any(metric not in self.metrics_to_calc for metric in type_metrics):
            raise ValueError(f"Current metrics implemented: {list(self.metrics_to_calc.keys())}" \
                + "\tOne of the metrics in parameter not implemented")

        args= self._get_numbers(found=found, gold_standard=gold_standard)
        _metrics = {metric: f(**args) \
            for metric, f in self.metrics_to_calc.items()}
        return _metrics


if __name__ == '__main__':
    import os
    import argparse
    import pandas as pd
    from os import listdir
    from settings import FOLDER_PATH
    metrics = Metrics(referent_path=os.path.join(FOLDER_PATH, "referents.json"))

    df_gs = pd.read_csv(os.path.join(FOLDER_PATH, "test.csv"))
    event_gs = list(df_gs[df_gs['linkDBpediaEn']!=''].linkDBpediaEn.unique())
    print(len(event_gs))
    TYPE_METRICS=['precision', 'recall', 'f1']

    ap = argparse.ArgumentParser()
    ap.add_argument("-f", "--folder", required=True,
                    help="Folder with subgraph and pending nodes at each iteration")
    FOLDER = vars(ap.parse_args())["folder"]

    for iteration in range(len(listdir(FOLDER))//3 - 1):
        events_found = list(set(\
            [e for e in pd.read_csv(f"{FOLDER}/{iteration+1}-subgraph.csv") \
                .subject.values if isinstance(e, str)]))
        res = metrics(found=events_found, gold_standard=event_gs, type_metrics=TYPE_METRICS)
        print(res)
