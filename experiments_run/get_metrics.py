# -*- coding: utf-8 -*-
"""
Getting metrics for SEM comparison between ground truth KGs and search KGs
"""
import os
import json
import click
from rdflib import Graph
from tqdm import tqdm
from datetime import datetime
from loguru import logger
from src.build_ng.sem_comparison import SEMComparer

INFO_DATASET = ["dbpedia", "wikidata"]

def get_metrics(comparer, folder: str, graph_c_path: str, graph_gs_path: str) -> dict:
    # eventkg_ng, generation_ng, search_ng
    graph_c = Graph()
    graph_c.parse(os.path.join(folder, graph_c_path), format="ttl")

    graph_gs = Graph()
    graph_gs.parse(os.path.join(folder, graph_gs_path), format="ttl")
    return comparer(graph_c=graph_c, graph_gs=graph_gs)


@click.command()
@click.option("--folder", help="Folder to build the NGs")
@click.option("--output_name", help="name of output file with metrics")
@click.option("--graph_c_path", help="name of file for constructed graph")
@click.option("--graph_gs_path", help="name of file for ground truth graph")
def main(folder: str, output_name: str, graph_c_path: str, graph_gs_path: str):
    start_time = datetime.now()
    comparer = SEMComparer()
    logger.info(f"[Time] Started at: {start_time}")
    datasets = [x for x in os.listdir(folder) if x in INFO_DATASET]
    for dataset in datasets:
        logger.info(f"### Dataset: {dataset.upper()}")
        exps = os.listdir(os.path.join(folder, dataset))
        nb_exp = len(exps)

        for i, exp in enumerate(exps):
            perc = round(100*i/nb_exp, 1)
            logger.info(f"[Dataset] {dataset}\t({perc}%)\t[Event] {exp}")

            curr_folder = os.path.join(folder, dataset, exp)
            if all(x in os.listdir(curr_folder) for x in [graph_gs_path, graph_c_path]):  # Checking that KGs were generated
                save_file = os.path.join(curr_folder, output_name)
                if not os.path.exists(save_file):  # Checking that metrics not already generated
                    logger.info("[WIP] Computing metrics")
                    metrics = get_metrics(comparer=comparer, folder=curr_folder, graph_c_path=graph_c_path, graph_gs_path=graph_gs_path)
                    json.dump(metrics, open(save_file, "w", encoding='utf-8'), indent=4)
                    logger.success("[Done] Computed and saved metrics")
                else:
                    logger.success("[Done] Metrics already saved")
    
    end_time = datetime.now()
    logger.info(f"[Time] Ended at: {end_time}")
    logger.info(f"[Time] Took: {end_time - start_time}")


if __name__ == '__main__':
    main()
