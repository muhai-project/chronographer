"""
#TO DO: add documentation on this script
"""
import os
import json
import time
import multiprocessing as mp
from datetime import datetime
from collections import defaultdict
import yaml
from ray.util.multiprocessing import Pool

import pandas as pd
from settings import FOLDER_PATH
from src.ranker import Ranker
from src.metrics import Metrics
from src.plotter import Plotter
from src.ordering import Ordering
from src.expansion import NodeExpansion
from src.hdt_interface import HDTInterface
from src.triply_interface import TriplInterface
from doc.check_config_framework import CONFIG_TYPE_ERROR_MESSAGES \
    as config_error_messages


class GraphSearchFramework:
    """
    #TO DO: add documentation on this script
    """
    def __init__(self, config: dict):
        """
        Type of ranking strategies implemented:
            - pred_freq:
            - entropy_pred_freq:
            - inverse_pred_freq:
            - pred_object_freq:
            - entropy_pred_object_freq:
            - inverse_pred_object_freq:
            - not implemented: subject_freq:
            - not implemented: inverse_subject_freq:
            - not implemented: inverse_pred_object_split_freq:
        """
        self.possible_type_interface = ["triply", "hdt"]
        self.possible_type_ranking = [
            "pred_freq", "inverse_pred_freq", "entropy_pred_freq",
            "pred_object_freq", "inverse_pred_object_freq", "entropy_pred_object_freq"]
        self.possible_type_metrics = ["precision", "recall", "f1"]
        self.config_error_messages = config_error_messages

        self._check_config(config=config)
        self.iterations = config["iterations"]
        self.type_interface = config["type_interface"]

        self.dataset_type = config["dataset_type"]
        with open(
            os.path.join(FOLDER_PATH, "dataset-config", f"{config['dataset_type']}.yaml"),
            encoding='utf-8') as file:
            self.dataset_config = yaml.load(file, Loader=yaml.FullLoader)

        self.type_ranking = config["type_ranking"]
        self.folder_name_suffix = \
            self.get_exp_name(config=config)
        self.save_folder = self._add_save_info()

        self.config = config
        self.rdf_type = config["rdf_type"]
        self.predicate_filter = config["predicate_filter"]
        self.start = config["start"]

        self.dates = [config["start_date"], config["end_date"]]
        self.name_exp = config["name_exp"]

        if "exclude_category" in config:
            filter_kb = config["exclude_category"]
        else:
            filter_kb = 1
        if self.type_interface == "triply":
            self.interface = TriplInterface()
        else:  # type_interface == "hdt"
            nested = config["nested_dataset"] if "nested_dataset" in config else 1
            pred = self.dataset_config["point_in_time"] + self.dataset_config["start_dates"] + \
                self.dataset_config["end_dates"] + [self.dataset_config["rdf_type"]]
            self.interface = HDTInterface(filter_kb=filter_kb, folder_hdt=config["dataset_path"],
                                          dataset_config=self.dataset_config, nested_dataset=nested,
                                          default_pred=pred)

        self.subgraph = pd.DataFrame(columns=[
            "subject", "predicate", "object", "type_df", "iteration"])
        self.subgraph_info = {}

        self.pending_nodes_ingoing = pd.DataFrame(columns=["subject", "predicate", "object"])
        self.pending_nodes_outgoing = pd.DataFrame(columns=["subject", "predicate", "object"])
        self.info = pd.DataFrame(columns=["path", "iteration", "tot"] + \
            [x for elt in self.rdf_type for x in [f"{elt[0]}"] ])

        self.nb_cpu = mp.cpu_count()
        self.paths = []

        self.ranker = Ranker(type_ranking=self.type_ranking)
        self.nodes_expanded = []
        self.occurence = defaultdict(int)
        self.to_expand = None
        self.score_expansion = None
        self.nodes_expanded_per_iter = pd.DataFrame(columns=["iteration", "node_expanded"])
        self.expanded = pd.DataFrame(columns=["iteration", "path_expanded"])
        self.discarded = pd.DataFrame(columns=["iteration", "node_discarded"])

        self.metrics = Metrics(config["referents"])
        self.type_metrics = config["type_metrics"]
        df_gs = pd.read_csv(config['gold_standard'])
        self.event_gs = list(df_gs[df_gs['linkDBpediaEn']!=''].linkDBpediaEn.unique())
        self.metrics_data = {}

        self.plotter = Plotter()


        ordering_domain_range = config["ordering"]["domain_range"] if \
            "ordering" in config and "domain_range" in config["ordering"] else 0
        self.ordering = Ordering(interface=self.interface,
                                 domain_range=ordering_domain_range,
                                 focus_for_search=[x[1] for x in config["rdf_type"]])

        if "filtering" in config and "what" in config["filtering"] and \
            config["filtering"]["what"]:
            self.predicate_filter += [self.dataset_config["rdf_type"]]


        self.node_expander = NodeExpansion(rdf_type=self.rdf_type,
                                           interface=self.interface,
                                           args_filtering=self.get_config_filtering(
                                            config=config, dataset_config=self.dataset_config))

    @staticmethod
    def get_config_filtering(config: dict, dataset_config: dict):
        """ Create config for Filtering module in NodeExpansion """
        filtering_when = config["filtering"]["when"] if \
            "filtering" in config and "when" in config["filtering"] else 0
        filtering_where = config["filtering"]["where"] if \
            "filtering" in config and "where" in config["filtering"] else 0
        filtering_who = config["filtering"]["who"] if \
            "filtering" in config and "who" in config["filtering"] else 0

        return {
            "when": filtering_when,
            "where": filtering_where,
            "who": filtering_who,
            "point_in_time": dataset_config["point_in_time"],
            "start_dates": dataset_config["start_dates"],
            "end_dates": dataset_config["end_dates"],
            "places": dataset_config["places"],
            "people": dataset_config["person"],
            "dataset_type": dataset_config["config_type"],
        }

    def _check_config(self, config: dict):
        if not isinstance(config, dict):
            raise TypeError("`config` param type should be dict`")

        if "rdf_type" not in config:
            raise ValueError(self.config_error_messages['rdf_type'])
        if not isinstance(config["rdf_type"], list) or \
            any(not isinstance(elt, tuple) for elt in config["rdf_type"]) or \
            any(not isinstance(k, str) \
            or not isinstance(v, str) for k, v in config['rdf_type']):
            raise TypeError(self.config_error_messages['rdf_type'])

        if "predicate_filter" not in config:
            raise ValueError(self.config_error_messages['predicate_filter'])
        if not isinstance(config["predicate_filter"], list) or \
            any(not isinstance(elt, str) for elt in config["predicate_filter"]):
            raise TypeError(self.config_error_messages['predicate_filter'])

        if "start" not in config:
            raise ValueError(self.config_error_messages['start'])
        if not isinstance(config["start"], str):
            raise TypeError(self.config_error_messages['start'])

        if "iterations" not in config:
            raise ValueError(self.config_error_messages['iterations'])
        if not isinstance(config["iterations"], int):
            raise TypeError(self.config_error_messages['iterations'])

        if "type_ranking" not in config:
            raise ValueError(self.config_error_messages['type_ranking'])
        if config["type_ranking"] not in self.possible_type_ranking:
            raise TypeError(self.config_error_messages['type_ranking'])

        if "type_interface" not in config:
            raise ValueError(self.config_error_messages['type_interface'])
        if config["type_interface"] not in self.possible_type_interface:
            raise TypeError(self.config_error_messages['type_interface'])

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

        for date in ["start_date", "end_date"]:
            if date not in config:
                raise ValueError(self.config_error_messages[date])
            try:
                datetime(int(config[date][:4]), int(config[date][5:7]), int(config[date][8:10]))
            except Exception as type_error:
                raise TypeError(self.config_error_messages[date]) from type_error

        for k_p, v_p in [
            ("ordering", "domain_range"), ("filtering", "what"),
            ("filtering", "when"), ("filtering", "where")
        ]:

            if k_p in config and \
                isinstance(config[k_p], dict) and v_p in config[k_p]:
                if config[k_p][v_p] not in [0, 1]:
                    raise TypeError(self.config_error_messages[k_p][v_p])

        if "name_exp" not in config:
            raise ValueError(self.config_error_messages['name_exp'])
        if not isinstance(config["name_exp"], str):
            raise TypeError(self.config_error_messages['name_exp'])

        if "dataset_type" not in config:
            raise ValueError(self.config_error_messages['dataset_type'])
        if config["dataset_type"] not in ["wikidata", "dbpedia", "yago"]:
            raise TypeError(self.config_error_messages['dataset_type'])

        if "dataset_path" not in config:
            raise ValueError(self.config_error_messages['dataset_path'])
        if not isinstance(config["dataset_path"], str):
            raise TypeError(self.config_error_messages['dataset_path'])


    def get_exp_name(self, config):
        """ Get experiment name, depending on parameters """
        elts = [config['dataset_type'], config['name_exp'],
                str(config["iterations"]), config["type_ranking"]]
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

        if self.dataset_type == "dbpedia":  # wikilink for DBpedia only
            wikilink = "wikilink" if "http://dbpedia.org/ontology/wikiPageWikiLink" \
                in config["predicate_filter"] else ""
            elts.append(wikilink)
        cat = "with_category" if config.get("exclude_category") == 0 else "without_category"
        elts.append(cat)
        return "_".join(elts)

    def select_nodes_to_expand(self):
        """ Accessible call to _select_nodes_to_expand"""
        return self._select_nodes_to_expand()

    def _select_nodes_to_expand(self):
        if self.to_expand:
            for elt in ["1-", '2-', "3-"]:
                if self.to_expand.startswith(elt):
                    self.to_expand = self.to_expand.replace(elt, "")
            path = [self.to_expand]

            # Gettings args for next iteration
            if (";" in self.to_expand) and ("ingoing" in self.to_expand):
                pred, obj = self.to_expand.replace('ingoing-', '').split(";")
                nodes = list(
                    self.pending_nodes_ingoing[
                        (self.pending_nodes_ingoing.predicate.isin([pred, str(pred)])) & \
                        (self.pending_nodes_ingoing.object.isin([obj, str(obj)]))].subject.values)
            elif (";" in self.to_expand) and ("outgoing" in self.to_expand):
                subj, pred = self.to_expand.replace('outgoing-', '').split(";")
                nodes = list(
                    self.pending_nodes_outgoing[
                        (self.pending_nodes_outgoing.predicate.isin([pred, str(pred)])) & \
                        (self.pending_nodes_outgoing.subject \
                            .isin([subj, str(subj)]))].object.values)
            else:
                # print(self.pending_nodes_ingoing.predicate.unique())
                nodes = list(self.pending_nodes_ingoing[\
                    self.pending_nodes_ingoing.predicate.isin(
                        [self.to_expand])].subject.values) + \
                        list(self.pending_nodes_outgoing[\
                    self.pending_nodes_outgoing.predicate.isin(
                        [self.to_expand])].object.values)
                # print(nodes)

        else:  # INIT state: only starting node
            path, nodes = [], [self.start]

        return [node for node in nodes if node not in self.nodes_expanded], path

    def _expand_one_node(self, args: dict):
        return self.node_expander(args=args, dates=self.dates)

    def _update_nodes_expanded(self, iteration:int, nodes: list[str]):

        self.nodes_expanded_per_iter = pd.concat(
            [self.nodes_expanded_per_iter,
             pd.DataFrame([[iteration, nodes]], columns=["iteration", "node_expanded"])],
            ignore_index=True
        )

    def run_one_iteration(self, iteration: int):
        """ Running one iteration of the search framework """
        nodes_to_expand, path = self._select_nodes_to_expand()
        self._update_nodes_expanded(iteration=iteration, nodes=nodes_to_expand)

        if self.type_interface == '':
            pool = Pool(self.nb_cpu)
            output = pool.map(self._expand_one_node,
                                [{
                                    "node": node,
                                    "path": path,
                                    "predicate": self.predicate_filter,
                                    "iteration": iteration
                                    } for node in nodes_to_expand])
            pool.close()
            pool.join()

        else:  # type_interface == 'hdt'
            output = []
            for i, args in enumerate([{"node": node,
                                    "path": path,
                                    "predicate": self.predicate_filter,
                                    "iteration": iteration,
                                    } for node in nodes_to_expand]):
                print(f"Processing node {i+1}/{len(nodes_to_expand)}\t{nodes_to_expand[i]}")
                self.nodes_expanded.append(args["node"])
                output.append(self._expand_one_node(args))

        return output

    def update_occurence(self, ingoing: pd.core.frame.DataFrame,
                         outgoing: pd.core.frame.DataFrame, occurence: dict):
        """ Accessible call to _update_occurence """
        return self._update_occurence(ingoing, outgoing, occurence)

    def _get_nb(self, superclass, pred):
        if any(x in superclass for x in [y[1] for y in self.rdf_type]):
            return "1"
        if pred in []:
            return "2"
        return "3"

    def _update_occurence(self, ingoing: pd.core.frame.DataFrame,
                          outgoing: pd.core.frame.DataFrame, occurence: dict):
        """
        Updating occurences for future path ranking
        In any case: adding info about the type of predicate
        1 = relevant superclass | 2 = filtered predicate | 3 = other
        - If path on predicate only: {1-3} + pred
        - If pred_object: adding whether ingoing or outgoing
        """
        if self.type_ranking in ["pred_freq", "entropy_pred_freq",
                                 "inverse_pred_freq"]:  # predicate
            for _, row in ingoing.iterrows():
                nb_order = self._get_nb(superclass=row.superclass, pred=row.predicate)
                occurence[f"{nb_order}-{str(row.predicate)}"] += 1
            for _, row in outgoing.iterrows():
                nb_order = self._get_nb(superclass=row.superclass, pred=row.predicate)
                occurence[f"{nb_order}-{str(row.predicate)}"] += 1
        if self.type_ranking in ["pred_object_freq",
                                 "entropy_pred_object_freq",
                                 "inverse_pred_object_freq"]:  # subject predicate
            for _, row in ingoing.iterrows():
                nb_order = self._get_nb(superclass=row.superclass, pred=row.predicate)
                occurence[f"{nb_order}-ingoing-{str(row.predicate)};{str(row.object)}"] += 1
            for _, row in outgoing.iterrows():
                nb_order = self._get_nb(superclass=row.superclass, pred=row.predicate)
                occurence[f"{nb_order}-outgoing-{str(row.subject)};{str(row.predicate)}"] += 1
        return occurence


    def merge_outputs(self, output: list, iteration: int, info: dict):
        """ Gather outputs from each of the nodes expanded """
        curr_discarded = []
        for subgraph_ingoing, path_ingoing, subgraph_outgoing, path_outgoing, to_discard in output:
            subgraph_ingoing["iteration"] = iteration
            subgraph_outgoing["iteration"] = iteration
            self._merge_outputs_single_run(subgraph_ingoing, path_ingoing,
                                           subgraph_outgoing, path_outgoing, info, iteration)

            curr_discarded += to_discard

        self.discarded = pd.concat(
            [self.discarded,
             pd.DataFrame([[iteration, list(set(curr_discarded))]],
                          columns=["iteration", "node_discarded"])],
            ignore_index=True
        )

        self.to_expand, self.score_expansion = self.ranker(occurences=self.occurence)
        if self.to_expand:
            self.occurence = defaultdict(int, {k:v for k, v in self.occurence.items() \
                if k != self.to_expand})
            self.pending_nodes_ingoing = self.pending_nodes_ingoing[\
                ~self.pending_nodes_ingoing.subject.isin(self.nodes_expanded)]
            self.pending_nodes_outgoing = self.pending_nodes_outgoing[\
                ~self.pending_nodes_outgoing.object.isin(self.nodes_expanded)]

        return info

    def _merge_outputs_single_run(self, subgraph_ingoing: pd.core.frame.DataFrame,
                                  path_ingoing: pd.core.frame.DataFrame,
                                  subgraph_outgoing: pd.core.frame.DataFrame,
                                  path_outgoing: pd.core.frame.DataFrame,
                                  info: dict, iteration: int):
        self.subgraph = pd.concat([self.subgraph, subgraph_ingoing], axis=0)
        self.subgraph = pd.concat([self.subgraph, subgraph_outgoing], axis=0)

        # Pre-ordering step (remove non relevant predicates)
        # 1st = add info on predicates (using domain/range information)
        path_ingoing, info = self.ordering(triple_df=path_ingoing, type_node="ingoing",
                                            info=info, iteration=iteration)
        path_outgoing, info = self.ordering(triple_df=path_outgoing, type_node="outgoing",
                                            info=info, iteration=iteration)

        self.pending_nodes_ingoing = pd.concat(
            [self.pending_nodes_ingoing, path_ingoing], axis=0)
        self.pending_nodes_outgoing = pd.concat(
            [self.pending_nodes_outgoing, path_outgoing], axis=0)
        # self.info = pd.concat([self.info, info], axis=0)

        self.occurence = self._update_occurence(ingoing=path_ingoing,
                                                outgoing=path_outgoing,
                                                occurence=self.occurence)


    def _add_save_info(self):
        date_begin = datetime.now()
        date = '-'.join([str(date_begin)[:10], str(date_begin)[11:19]])

        folder_path = os.path.join(FOLDER_PATH, "experiments")
        if not os.path.exists(folder_path):
            os.makedirs(folder_path)

        save_folder = os.path.join(folder_path,
                                 f"{date}-{self.folder_name_suffix}")
        if os.path.exists(save_folder):
            raise ValueError("Folder to save data already exists, re creating one")
        os.makedirs(save_folder)
        return save_folder

    def update_metrics(self, iteration, found):
        """ Update metrics after one iteration """
        self.metrics_data[iteration] = \
            self.metrics(found=found, gold_standard=self.event_gs,
                         type_metrics=self.type_metrics)

    def add_subgraph_info(self, iteration):
        """ Tracking # of events + unique events found """
        size = self.subgraph.shape[0]
        unique = len(set([str(e) for e in self.subgraph[self.subgraph.type_df == "ingoing"] \
                    .subject.unique()] + \
                    [str(e) for e in self.subgraph[self.subgraph.type_df == "outgoing"] \
                        .object.unique()]))
        self.subgraph_info[iteration] = dict(subgraph_nb_event=size,
                                             subgraph_nb_event_unique=unique)


    def _udpate_metadata(self, metadata):
        last_metrics = self.metrics_data[self.iterations]
        metadata.update({
            "end": str(datetime.now()),
            "last_f1":  last_metrics["f1"],
            "last_precision":  last_metrics["precision"],
            "last_recall":  last_metrics["recall"],
        })
        return metadata

    def _update_best(self, metadata, iteration):
        best_metrics = self.metrics_data[iteration]
        metadata.update({
            "best_f1": best_metrics['f1'],
            "best_corresponding_precision": best_metrics['precision'],
            "best_corresponding_recall": best_metrics['recall'],
            "best_f1_it_nb": iteration
        })
        return metadata
    
    def _update_last(self, metadata, iteration):
        last_metrics = self.metrics_data[iteration]
        metadata.update({
            "end": str(datetime.now()),
            "last_f1":  last_metrics["f1"],
            "last_precision":  last_metrics["precision"],
            "last_recall":  last_metrics["recall"],
            "last_it": iteration
        })
        return metadata

    def __call__(self):
        start = datetime.now()
        metadata = {"start": str(start)}
        with open(f"{self.save_folder}/config.json", "w", encoding='utf-8') as openfile:
            json.dump(self.config, openfile,
                      indent=4)
        self.expanded = pd.DataFrame(columns=["iteration", "path_expanded", "score"])
        self.metrics_data = {}
        self.info = {}
        best_fone = 0

        for i in range(1, self.iterations+1):
            print(f"Iteration {i} started at {datetime.now()}")
            output = self.run_one_iteration(iteration=i)
            self.info = self.merge_outputs(output=output, iteration=i, info=self.info)

            self.add_subgraph_info(iteration=i)
            self.subgraph.to_csv(f"{self.save_folder}/{i}-subgraph.csv")

            self.pending_nodes_ingoing.to_csv(
                f"{self.save_folder}/{i}-pending_nodes_ingoing.csv")
            self.pending_nodes_outgoing.to_csv(
                f"{self.save_folder}/{i}-pending_nodes_outgoing.csv")

            with open(f"{self.save_folder}/{i}-occurences.json", "w", encoding='utf-8') \
                    as openfile:
                json.dump(self.occurence, openfile,
                            indent=4)
            self.expanded.to_csv(f"{self.save_folder}/expanded.csv")


            events_found = \
                [str(e) for e in self.subgraph[self.subgraph.type_df == "ingoing"] \
                    .subject.unique()] + \
                    [str(e) for e in self.subgraph[self.subgraph.type_df == "outgoing"] \
                        .object.unique()]
            self.update_metrics(iteration=i, found=events_found)

            with open(f"{self.save_folder}/metrics.json", "w", encoding='utf-8') as openfile:
                json.dump(self.metrics_data, openfile,
                            indent=4)
            with open(f"{self.save_folder}/info.json", "w", encoding='utf-8') as openfile:
                json.dump(self.info, openfile,
                            indent=4)

            if self.metrics_data[i]["f1"] > best_fone:
                metadata = self._update_best(metadata, i)
                best_fone = self.metrics_data[i]["f1"]
            metadata = self._update_last(metadata, i)
            with open(f"{self.save_folder}/metadata.json", "w", encoding="utf-8") as openfile:
                json.dump(metadata, openfile, indent=4)

            print(f"Iteration {i} finished at {datetime.now()}\n=====")

            with open(f"{self.save_folder}/metrics.json", "r", encoding="utf-8") as openfile:
                self.plotter(info=json.load(openfile), save_folder=self.save_folder)

            if self.to_expand:

                self.expanded = pd.concat(
                    [self.expanded, pd.DataFrame([[i, self.to_expand, self.score_expansion]],
                    columns=["iteration", "path_expanded", "score"])],
                    ignore_index=True
                )

                self.expanded.to_csv(f"{self.save_folder}/expanded.csv")

            else:
                print("According to params, no further nodes to expand," \
                    + f"finishing process at {datetime.now()}\n=====")
                break

        with open(f"{self.save_folder}/metadata.json", "w", encoding="utf-8") as openfile:
            json.dump(metadata, openfile, indent=4)



if __name__ == '__main__':
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("-j", "--json", required=True,
                    help="Path to json file containing configuration file")
    json_path = vars(ap.parse_args())["json"]

    with open(json_path, "r", encoding="utf-8") as openfile_main:
        config_loaded = json.load(openfile_main)
    config_loaded["rdf_type"] = list(config_loaded["rdf_type"].items())

    framework = GraphSearchFramework(config=config_loaded)
    start = datetime.now()
    print(f"Process started at {start}")
    framework()
    end = datetime.now()
    print(f"Process ended at {end}, took {end-start}")
    # print(framework.ordering.superclasses)
    # print(framework.ordering.domain)
    # print(framework.ordering.range)
