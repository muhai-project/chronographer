# -*- coding: utf-8 -*-
"""
Building all NGs from search output

- For comparison with EventKG, only keeping true positives
"""
import os
import json
import click
import subprocess
import pandas as pd
from tqdm import tqdm
from datetime import datetime
from loguru import logger
from src.helpers.data_load import read_csv
from src.build_ng.generic_kb_to_ng import KGConverter
from src.build_ng.eventkg_to_ng import EventKGToNGConverter

INFO_DATASET = {
    "wikidata": {"fs": "wikidata", "fng": "wikidata"},
    "dbpedia": {"fs": "/dbpedia", "fng": "dbpedia_en"}
}
EVENTKG_CONVERTER = EventKGToNGConverter()

def build_ng_search(events, folder, converter):
    """ Narrative Graph from search output """
    # filter search output
    output_search = read_csv(path=os.path.join(folder, "output_search.csv"))
    output_search = output_search[
        ((output_search.type_df == "ingoing") & (output_search.subject.isin(events))) | \
            ((output_search.type_df == "outgoing") & (output_search.object.isin(events)))]
    
    # get start/end dates
    with open(os.path.join(folder, "config.json"), encoding='utf-8') as openfile:
        config = json.load(openfile)
    
    graph = converter(input_df=output_search, start_d=config["start_date"], end_d=config["end_date"])
    return graph


@click.command()
@click.option("--folder", help="Folder to build the NGs")
def main(folder):
    start_time = datetime.now()
    logger.info(f"[Time] Started at: {start_time}")
    datasets = [x for x in os.listdir(folder) if x in INFO_DATASET]
    for dataset in datasets:
        logger.info(f"### Dataset: {dataset.upper()}")
        generic_kb_converter = KGConverter(dataset=dataset)
        exps = os.listdir(os.path.join(folder, dataset))
        nb_exp = len(exps)

        for i, exp in enumerate(exps):
            perc = round(100*(i+1)/nb_exp, 1)
            logger.info(f"[Dataset] {dataset}\t({perc}%)\t[Event] {exp}")

            curr_folder = os.path.join(folder, dataset, exp)
            gs_events = os.path.join(curr_folder, "gs_events.csv")

            if not os.path.exists(os.path.join(curr_folder, "ng_build.txt")):
                f = open(os.path.join(curr_folder, "ng_build.txt"), "w+")
            else:
                f = None
            # common_events = os.path.join(curr_folder, "common_events.txt")
            
            if (os.path.exists(gs_events)):
                events = read_csv(gs_events).linkDBpediaEn.values
                # events = open(gs_events).read().split("\n")
                with open(os.path.join(curr_folder, "config.json"), encoding='utf-8') as openfile:
                    config = json.load(openfile)

                # Build NG with generation + ground truth events
                start = datetime.now()
                generation_all_kg_file = os.path.join(curr_folder, "generation_ng.ttl")
                if not os.path.exists(generation_all_kg_file):
                    logger.info(f"[{dataset}][{exp}][WIP] Building NG from generation + all GS")
                    input_df = pd.DataFrame({"subject": events, "predicate": ["partof"]*len(events), "object": events, "type_df": ["ingoing"]*len(events)})
                    graph = generic_kb_converter(input_df=input_df, start_d=config["start_date"], end_d=config["end_date"])
                    graph.serialize(generation_all_kg_file, format="ttl")
                    logger.success(f"[{dataset}][{exp}][Done] Built NG from generation + all GS")
                else:
                    logger.success(f"[{dataset}][{exp}][Done] NG from generation + all GS already built")
                end = datetime.now()
                if f:
                    f.write(f"Generation + GS took:\t{end-start}\n")

                # Build NG from graph search output
                start = datetime.now()
                kg_file = os.path.join(curr_folder, "search_ng.ttl")
                output_search_file = os.path.join(curr_folder, "output_search.csv")
                metadata_file = os.path.join(curr_folder, "metadata.json")
                if not os.path.exists(kg_file):
                    logger.info(f"[{dataset}][{exp}][WIP] Building NG from search output")
                    if os.path.exists(output_search_file) and os.path.exists(metadata_file):
                        output_search = read_csv(path=output_search_file)

                        # Output search from best iteration
                        with open(metadata_file, encoding='utf-8') as openfile:
                            metadata = json.load(openfile)
                        best_it = int(metadata["best_f1_it_nb"])
                        output_search = output_search[output_search.iteration <= best_it]
                        graph = generic_kb_converter(input_df=output_search, start_d=config["start_date"], end_d=config["end_date"])
                        # graph = build_ng_search(events=events, folder=curr_folder,
                        #                         converter=generic_kb_converter)
                        graph.serialize(kg_file, format="ttl")
                        logger.success(f"[{dataset}][{exp}][Done] Built NG from search output")
                    else:
                        logger.warning(f"[{dataset}][{exp}][Aborted] no output search")
                else:
                    logger.success(f"[{dataset}][{exp}][Done] NG from search output already built")
                end = datetime.now()
                if f:
                    f.write(f"Search output took:\t{end-start}\n")
                
            
                # Build NG from EventKG
                start = datetime.now()
                kg_file = os.path.join(curr_folder, "eventkg_ng.ttl")
                if not os.path.exists(kg_file):
                    logger.info(f"[{dataset}][{exp}][WIP] Building NG from EventKG")
                    graph = EVENTKG_CONVERTER(events=events, filter_str=INFO_DATASET[dataset]["fs"],
                                                filter_named_graph=INFO_DATASET[dataset]["fng"])
                    graph.serialize(kg_file, format="ttl")
                    logger.success(f"[{dataset}][{exp}][Done] Built NG from EventKG")
                else:
                    logger.success(f"[{dataset}][{exp}][Done] NG from EventKG already built")
                end = datetime.now()
                if f:
                    f.write(f"EventKG took:\t{end-start}\n")
                    f.close()
    
    end_time = datetime.now()
    logger.info(f"[Time] Ended at: {end_time}")
    logger.info(f"[Time] Took: {end_time - start_time}")
    logger.add(os.path.join(folder, "ng_build.log"))


if __name__ == '__main__':
    main()
