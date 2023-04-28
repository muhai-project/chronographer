# -*- coding: utf-8 -*-
"""
Metrics for assessing the quality of the output
"""
import json
import pandas as pd

from doc.check_config_framework import CONFIG_TYPE_ERROR_MESSAGES \
    as config_error_messages

class Metrics:
    """
    Quantitative metrics: precision, recall, f1
    """

    def __init__(self, config_metrics: dict):
        """ config_metrics should have the following keys:
        cf. doc/check_config_framework.py for indications
        - `referents`:
        - `type_metrics`
        - `gold_standard`:
        """
        self.config_error_messages = config_error_messages
        self.possible_type_metrics = ["precision", "recall", "f1"]
        self._check_config(config=config_metrics)

        self.metrics_to_calc = {
            'precision': self.get_precision,
            'recall': self.get_recall,
            'f1': self.get_f1,
        }

        self.type_metrics = config_metrics["type_metrics"]
        df_gs = pd.read_csv(config_metrics['gold_standard'])
        self.event_gs = list(df_gs[df_gs['linkDBpediaEn']!=''].linkDBpediaEn.unique())

        with open(config_metrics["referents"], "r", encoding='utf-8') as openfile:
            self.referents = json.load(openfile)

    def _check_config(self, config: dict):
        if "gold_standard" not in config:
            raise ValueError(self.config_error_messages['gold_standard'])
        try:
            pd.read_csv(config["gold_standard"])['linkDBpediaEn']
        except Exception as type_error:
            raise TypeError(self.config_error_messages['gold_standard']) from type_error

        if "referents" not in config:
            raise ValueError(self.config_error_messages['referents'])
        try:
            with open(config["referents"], "r", encoding='utf-8') as openfile:
                json.load(openfile)
        except Exception as type_error:
            raise TypeError(self.config_error_messages['referents']) from type_error

        if "type_metrics" not in config:
            raise ValueError(self.config_error_messages['type_metrics'])
        if not isinstance(config['type_metrics'], list) or \
            any(elt not in self.possible_type_metrics for elt in config['type_metrics']):
            raise TypeError(self.config_error_messages['type_metrics'])

    @staticmethod
    def get_numbers(found: list[str], gold_standard: list[str]) -> dict:
        """ Numbers necessary to calculate the metrics """
        found, gold_standard = set(found), set(gold_standard)
        true_pos = len(found.intersection(gold_standard))
        false_pos = len(found.difference(gold_standard))
        false_neg = len(gold_standard.difference(found))
        return dict(true_pos=true_pos,
                    false_pos=false_pos,
                    false_neg=false_neg)

    @staticmethod
    def get_precision(**args: dict) -> float:
        """ Precision """
        if args["true_pos"] + args["false_pos"] == 0:
            return 0
        return args["true_pos"] / (args["true_pos"] + args["false_pos"])

    @staticmethod
    def get_recall(**args: dict) -> float:
        """ Recall """
        if args["true_pos"] + args["false_neg"] == 0:
            return 0
        return args["true_pos"] / (args["true_pos"] + args["false_neg"])

    @staticmethod
    def get_f1(**args: dict) -> float:
        """ f1 """
        if args["true_pos"] + \
            0.5 * (args["false_pos"] + args["false_neg"]) == 0:
            return 0
        return args["true_pos"] / (args["true_pos"] + \
            0.5 * (args["false_pos"] + args["false_neg"]))

    def update_metrics_data(self, metrics_data: dict, iteration: int,
                            found: list[str]) -> dict:
        """ Compute metrics for one iteration """
        metrics_data[iteration] = self.get_metrics(found=found)
        return metrics_data

    def get_metrics(self, found: list) -> dict:
        """ Calculate all metrics from found nodes (compared to ground truth) """
        def f_change(url):
            return self.referents[url] if url in self.referents else url
        found = [f_change(url) for url in found]

        if any(metric not in self.metrics_to_calc for metric in self.type_metrics):
            raise ValueError(f"Current metrics implemented: {list(self.metrics_to_calc.keys())}" \
                + "\tOne of the metrics in parameter not implemented")

        args= self.get_numbers(found=found, gold_standard=self.event_gs)
        _metrics = {metric: f(**args) \
            for metric, f in self.metrics_to_calc.items()}
        return _metrics

    def __call__(self, found: list, gold_standard: list,
                 type_metrics: list) -> dict:
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
    from settings import FOLDER_PATH

    CONFIG_METRICS = {
        "referents": os.path.join(FOLDER_PATH, "sample-data", "French_Revolution_referents.json"),
        "type_metrics": ['precision', 'recall', 'f1'],
        "gold_standard": os.path.join(FOLDER_PATH, "sample-data", "French_Revolution_gs_events.csv")
    }
    metrics = Metrics(config_metrics=CONFIG_METRICS)

    print("Event: French Revolution")
    print(f"# of sub-events: {len(metrics.event_gs)}")

    subgraph = pd.read_csv(os.path.join(
        FOLDER_PATH, "sample-data", "French_Revolution_subgraph.csv"))
    events_found = \
                [str(e) for e in subgraph[subgraph.type_df == "ingoing"] \
                    .subject.unique()] + \
                    [str(e) for e in subgraph[subgraph.type_df == "outgoing"] \
                        .object.unique()]
    res = metrics.get_metrics(found=events_found)
    print(res)
