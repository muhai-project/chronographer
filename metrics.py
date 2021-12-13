

class Metrics:

    def __init__(self):
        self.metrics_to_calc = {
            'precision': lambda tp, fp, fn: self._get_precision(tp, fp, fn),
            'recall': lambda tp, fp, fn: self._get_recall(tp, fp, fn),
            'f1': lambda tp, fp, fn: self._get_f1(tp, fp, fn),
        }

    def _get_numbers(self, found, gold_standard):
        found, gold_standard = set(found), set(gold_standard)
        tp = len(found.intersection(gold_standard))
        fp = len(found.difference(gold_standard))
        fn = len(gold_standard.difference(found))
        return tp, fp, fn
    
    def _get_precision(self, tp, fp, fn):
        return tp / (tp + fp)
    
    def _get_recall(self, tp, fp, fn):
        return tp / (tp + fn)
    
    def _get_f1(self, tp, fp, fn):
        return tp / (tp + 0.5 * (fp + fn))
    
    def __call__(self, found: list, gold_standard: list,
                 type_metrics: list = ['precision', 'recall', 'f1']):
        if any(metric not in self.metrics_to_calc for metric in type_metrics):
            raise ValueError(f"Current metrics implemented: {list(self.metrics_to_calc.keys())}\tOne of the metrics in parameter not implemented")
        
        tp, fp, fn = self._get_numbers(found=found, gold_standard=gold_standard)
        metrics = {metric: f(tp, fp, fn) for metric, f in self.metrics_to_calc.items()}
        return metrics


if __name__ == '__main__':
    import numpy as np
    import pandas as pd
    from os import listdir
    metrics = Metrics()

    # found = ["a", "b", "c", "d"]
    # gold_standard = ["a", "b", "c", "e", "f"]
    # type_metrics=['precision', 'recall', 'f1']

    event_gs = [e for e in pd.read_csv("events.csv").linkDBpediaEn.values if type(e) is str]
    type_metrics=['precision', 'recall', 'f1']

    folder = 'iter-20-event-entropy'

    for iteration in range(len(listdir(folder))//3):
        events_found = list(set([e for e in pd.read_csv(f"{folder}/{iteration+1}-subgraph.csv").subject.values if type(e) is str]))
        res = metrics(found=events_found, gold_standard=event_gs, type_metrics=type_metrics)
        print(res)
