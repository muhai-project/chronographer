# -*- coding: utf-8 -*-
"""
Averaging results for each experiment
"""
import os
import click
import json
import numpy as np
from src.build_table import build_table

INFO_DATASET = ["dbpedia", "wikidata"]
ROWS = ["all", "sem:hasActor", "sem:hasBeginTimeStamp", "sem:hasEndTimeStamp", "sem:hasPlace"]
ROWS_I = {row: i for i, row in enumerate(ROWS)}

COLUMNS = ["f1", "precision", "recall", "triples_common", "triples_gs_only", "triples_search_only"]
COLUMNS_J = {col: j for j, col in enumerate(COLUMNS)}
COL_TO_KEEP = 3

def get_table_one_exp(dict_result: dict) -> np.array:
    """ format of dict_results """
    res = np.zeros((len(ROWS), len(COLUMNS)))
    preds = dict_result["numbers"].keys()
    for pred in preds:
        # Filling array res row by row
        for key, val in dict_result["numbers"][pred].items():
            if pred in ROWS_I and key in COLUMNS_J:
                res[ROWS_I[pred]][COLUMNS_J[key]] = val
        for key, val in dict_result["metrics"][pred].items():
            if pred in ROWS_I and key in COLUMNS_J:
                res[ROWS_I[pred]][COLUMNS_J[key]] = val
    return res


@click.command()
@click.option("--folder", help="Folder to build the NGs")
@click.option("--metric", help="metric .json file (cf. get_metrics.py)")
@click.option("--label", help="label for overleaf table (eg., tab:metric-narrative-graph)")
def main(folder: str, metric: str, label: str):
    datasets = [x for x in os.listdir(folder) if x in INFO_DATASET]
    res = {x: [] for x in datasets}
    len_c, len_gs = [], []
    for dataset in datasets:
        exps = os.listdir(os.path.join(folder, dataset))
        for _, exp in enumerate(exps):
            metrics_file = os.path.join(folder, dataset, exp, metric)
            if os.path.exists(metrics_file):
                with open(metrics_file, encoding="utf-8") as openfile:
                    dict_result = json.load(openfile)
                table = get_table_one_exp(dict_result=dict_result)
                if list(table[0, :]) != [0] * 6:
                    res[dataset].append(table)
                
                len_c.append(dict_result["triples"]["len_c"])
                len_gs.append(dict_result["triples"]["len_gs"])

    data = {}
    # if mode == "overall":
    for dataset in datasets:
        # print(dataset)
        output = np.sum(res[dataset], axis=0)
        output[:, 1] = np.round(100*output[:, 3]/(output[:, 3] + output[:, 4]), decimals=1)
        output[:, 2] = np.round(100*output[:, 3]/(output[:, 3] + output[:, 5]), decimals=1)
        output[:, 0] = np.round(2*output[:, 1]*output[:, 2]/(output[:, 1]+output[:, 2]), decimals=1)
        data[dataset] = output
        print(output)
    table = np.zeros((len(ROWS), 2*len(COLUMNS)))
    for i, dataset in enumerate(INFO_DATASET):
        for j in range(len(COLUMNS)):
            print(table[:,2*j+i])
            table[:,2*j+i] = data[dataset][:,j]
    table = [list(x)[:COL_TO_KEEP*2] for x in list(table)]
    table = [["\\texttt{" + pred + "}"] + table[i] for i, pred in enumerate(ROWS)] 
    
    latex_table = build_table(
        columns=["Pred"] + [x.capitalize().replace("_", " ") for x in COLUMNS[:COL_TO_KEEP]],
        alignment="c" + "rr"*len(COLUMNS[:COL_TO_KEEP]),
        caption="Metrics of generated narrative graphs compared to EventKG.",
        label=label,
        position="h",
        data=table,
        sub_columns=[""] + ["DB", "WD"]*len(COLUMNS[:COL_TO_KEEP]),
        multicol=[1] + [2]*len(COLUMNS[:COL_TO_KEEP])
    )
    print(latex_table)

    avg_c = round(np.mean(np.array(len_c)))
    print(f"Average number of triples in c graph: {avg_c}")
    avg_gs = round(np.mean(np.array(len_gs)))
    print(f"Average number of triples in gs graph: {avg_gs}")
    

if __name__ == '__main__':
    main()