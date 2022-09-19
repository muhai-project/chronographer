""" Running grid search for one event"""
import os
import json
import time
import random
import psutil
import argparse
import multiprocessing
from multiprocessing import get_context

import pandas as pd
from copy import deepcopy
from sklearn.model_selection import ParameterGrid
from ray.util.multiprocessing.pool import Pool

from src.framework import GraphSearchFramework
from params_grid_search import PARAM_GRID
from settings import FOLDER_PATH

os.environ["RAY_OBJECT_STORE_ALLOW_SLOW_STORAGE"] = "1"


def update_config(config, args, iteration, dataset):
    """ Updating config for sweep (inline params) """
    config = deepcopy(config)

    config['type_ranking'] = args['type_ranking']
    config['iterations'] = int(iteration)
    if "ordering" not in config:
        config["ordering"] = {}
    config["ordering"]["domain_range"] = int(args['ordering_domain_range'])
    
    if "filtering" not in config:
        config["filtering"] = {}
    
    for elt in ["what", "where", "when", "who"]:
        config["filtering"][elt] = int(args[f"filtering_{elt}"])
    
    config["gold_standard"] = os.path.join(FOLDER_PATH, "data-test", dataset, "gs_events", config["gold_standard"])
    config["referents"] = os.path.join(FOLDER_PATH, "data-test", dataset, "referents", config["referents"])

    return config


def run_framework(framework):
    """ Calling graph search framework """
    framework()


def run_framework_process(config):
    config["rdf_type"] = list(config["rdf_type"].items())
    framework = GraphSearchFramework(config=config)
    # p = multiprocessing.Process(target=run_framework, name="graph-search", args=(framework,))
    # p.start()
    #time.sleep(1800)
    # p.terminate()
    # p.join()
    framework()

    seq = [int(elt.split('-')[0]) for elt in os.listdir(framework.save_folder) if elt.split('-')[0].isdigit()]
    if seq:
        max_iter = max(seq)
        for i in range(1, max_iter):
            for file_name in ["occurences.json", "pending_nodes_ingoing.csv", "pending_nodes_outgoing.csv", "subgraph.csv"]:
                os.remove(os.path.join(framework.save_folder, f"{i}-{file_name}"))
    
    if os.path.exists(os.path.join(framework.save_folder, "info.json")):
        os.remove(os.path.join(framework.save_folder, "info.json"))
    if os.path.exists(os.path.join(framework.save_folder, "metrics.html")):
        os.remove(os.path.join(framework.save_folder, "metrics.html"))


def get_experiments(folder, date, name_exp, iteration):
    files = os.listdir(folder)
    files = [x for x in files if \
        x[:10] >= date and \
            name_exp in x and \
                # f"{iteration}-occurences.json" in os.listdir(os.path.join(folder, x))]
                f"1-occurences.json" not in os.listdir(os.path.join(folder, x)) and \
                    len(os.listdir(os.path.join(folder, x))) == 8]
    return files


def helper(pattern, x):
    return 1 if pattern in x else 0


def filter_params(folder, files, param_grid):
    """ Removing parameters that were already tested and finished """
    res = []
    for file in files:
        res.append({
            "ordering_domain_range": helper("domain_range", file),
            "type_ranking": "entropy_pred_object_freq" if "entropy_pred_object" in file else "pred_object_freq",
            "filtering_what": helper("what", file),
            "filtering_where": helper("where", file),
            "filtering_when": helper("when", file),
            "filtering_who": helper("who", file),
        })
    return [x for x in param_grid if x not in res]


def get_args_grid_one_event(config, iteration, param_grid, date, name_exp, dataset):
    params = list(ParameterGrid(param_grid))
    files = get_experiments(folder=os.path.join(FOLDER_PATH, "experiments"),
                            date=date,
                            name_exp=name_exp,
                            iteration=iteration)
    params = filter_params(folder=os.path.join(FOLDER_PATH, "experiments"),
                           files=files, param_grid=params)
    return [update_config(config, param, iteration, dataset) for param in params]


def main(args_grid):
    with get_context("spawn").Pool(processes=psutil.cpu_count(logical=False)) as pool:
        pool.map(run_framework_process, args_grid)
        pool.close()
        pool.join()


if __name__ == '__main__':
    ap = argparse.ArgumentParser()
    ap.add_argument("-i", "--iterations", required=True,
                    help="Number of iterations for search")
    ap.add_argument("-d", "--dataset", required=True,
                    help="Type of dataset, either `dbpedia` or `wikidata`")
    ap.add_argument("-c", "--config", required=True,
                    help="Path to config file or csv to config files \n" + \
                         "If .json file: one single config file \n" + \
                         "If .csv file: one column for event, another for number of iterations")
    args_main = vars(ap.parse_args())

    if not args_main["iterations"].isdigit():
        raise ValueError("-i parameter must be integer")
    
    if args_main["config"].endswith('.json'):
        try:
            with open(args_main["config"], "r", encoding="utf-8") as openfile:
                CONFIG = json.load(openfile)
        except Exception as error:
            print(error)
            raise ValueError("Please check your json path for config file")

        args_grid = get_args_grid_one_event(config=CONFIG, iteration=args_main["iterations"], param_grid=PARAM_GRID,
                                            date="2022-07-25",
                                            name_exp=f"{args_main['dataset']}_{CONFIG['name_exp']}_{args_main['iterations']}",
                                            dataset=args_main["dataset"])
                                        
    elif args_main["config"].endswith(".csv"):
        df = pd.read_csv(args_main["config"])
        args_grid = []
        for _, row in df.iterrows():
            event_name = row['event_name'].split('/')[-1].split(".")[0]
            if os.path.exists(os.path.join(FOLDER_PATH, "data-test", row["dataset"], "config", f"{event_name}.json")):
                with open(os.path.join(FOLDER_PATH, "data-test", row["dataset"], "config", f"{event_name}.json"), 'r', encoding="utf-8") as openfile:
                    config = json.load(openfile)
                args_grid += get_args_grid_one_event(config=config, iteration=row["iterations"], param_grid=PARAM_GRID,
                                                    date="2022-07-25", name_exp=f"{row['dataset']}_{event_name}",
                                                    dataset=row["dataset"])

    else:
        raise ValueError("-c argument should be either a .json file with one config, or a .csv file with several event names")
        
    f = lambda x: 1 if x == "dbpedia" else 0
    # args_grid = sorted(args_grid, key=lambda x: x["filtering"]["what"] + x["filtering"]["where"] + \
    #                         x["ordering"]["domain_range"] + f(x["dataset_type"]),
    #                    reverse=True)
    random.shuffle(args_grid)

    def get_exp_name(config):
        """ Get experiment name, depending on parameters """
        config = deepcopy(config)
        elts = [config['name_exp'], str(config["iterations"]), config["type_ranking"]]
        domain_range = "domain_range" if \
            config.get('ordering') and \
                config.get('ordering').get('domain_range') \
                else ""
        elts.append(domain_range)
        if config.get('filtering'):
            what = "what" if \
                config.get('filtering').get('what') else ""
            where = "where" if \
                config.get('filtering').get('where') else ""
            when = "when" if \
                config.get('filtering').get('when') else ""
            who = "who" if \
                config.get('filtering').get('who') else ""
            elts += [what, where, when, who]
        wikilink = "wikilink_out" if "http://dbpedia.org/ontology/wikiPageWikiLink" \
            in config["predicate_filter"] else "wikilink_in"
        elts.append(wikilink)
        cat = "with_category" if config.get("exclude_category") == 0 else "without_category"
        elts.append(cat)
        return "_".join(elts)

    main(args_grid)

    # for elt in args_grid:
    #     print(elt["name_exp"], elt["filtering"], elt["ordering"], elt["iterations"])
    # print(len(args_grid))

