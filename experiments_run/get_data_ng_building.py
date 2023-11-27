# -*- coding: utf-8 -*-
""" 
Retrieve data from graph search experiment:
- Output of each search
- Ground truth for the configuration

Pre-filtering only most successful searches
- domain_range = when = what = where = who = 1
- heuristic = epof -> 'entropy_pred_object_freq'

Output in a folder
<output-folder>
    <event-1>
        output_search.csv
        ground_truth_events.csv 
        common_events.txt

Using tar and --wildcards to only extract subfolders and files from the *.tar file
```bash
tar -xzvf experiments.tar.gz  --wildcards '*entropy_pred_object_freq_domain_range_what_where_when_who*/config.json' '*entropy_pred_object_freq_domain_range_what_where_when_who*/*-subgraph.csv'
```
"""
import os
import json
import subprocess
import pandas as pd

def main(params):
    exps = os.listdir(params["input_folder"])
    # Filtering by date
    date_start, date_end = params["start_date"], params["end_date"]
    exps = [x for x in exps if x[:len(date_start)] >= date_start and x[:len(date_end)]]
    # Filtering by type of system (informed-epof)
    exps = [x for x in exps if all(fi in x for fi in params["folder_filter"])]

    os.makedirs(params["output_folder"])
    os.makedirs(os.path.join(params["output_folder"], "dbpedia"))
    os.makedirs(os.path.join(params["output_folder"], "wikidata"))

    for exp in exps:
        print(exp)
        # Copying info for each experiment: ground truth events, output subgraph
        curr_folder = os.path.join(params["input_folder"], exp)
        with open(os.path.join(curr_folder, "config.json"), encoding="utf-8") as openfile:
            config = json.load(openfile)
        name_exp, dataset = config["name_exp"], config["dataset_type"]

        with open(os.path.join(curr_folder, "metadata.json"), encoding="utf-8") as openfile:
            metadata = json.load(openfile)

        curr_save_folder = os.path.join(params["output_folder"], dataset, name_exp)
        os.makedirs(curr_save_folder)

        gs_file = os.path.join(params['folder_gs'], dataset, 'gs_events',f"{name_exp}.csv")
        command = f'cp "{gs_file}" "{curr_save_folder}/gs_events.csv"'
        subprocess.call(command, shell=True)

        for filename in ["config.json", "metadata.json"]:
            file_path = os.path.join(curr_folder, filename)
            command = f'cp "{file_path}" "{curr_save_folder}"'
            subprocess.call(command, shell=True)

        if "best_f1_it_nb" in metadata:
            best_it = metadata['best_f1_it_nb']
            
            subgraph_file = [x for x in os.listdir(curr_folder) if x.endswith("-subgraph.csv")][0]
            output_search = pd.read_csv(os.path.join(curr_folder, subgraph_file))
            output_search = output_search[[col for col in output_search.columns if col != "Unnamed: 0"]]
            output_search = output_search[output_search.iteration <= best_it]
            output_search.to_csv(os.path.join(curr_save_folder, "output_search.csv"))


if __name__ == '__main__':
    from settings import FOLDER_PATH
    PARAMS = {
        "input_folder": os.path.join(FOLDER_PATH, "experiments"),
        "output_folder": os.path.join(FOLDER_PATH, "data_ng_building"),
        "start_date": "2023-02-27-22:58:12",
        "end_date": "2023-04-12-12:03:05",
        "folder_filter": ["domain_range", "when", "what", "where", "who", "entropy_pred_object_freq"],
        "folder_gs": os.path.join(FOLDER_PATH, "data-test")
    }
    main(params=PARAMS)
