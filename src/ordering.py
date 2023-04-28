# -*- coding: utf-8 -*-
"""
Ordering class: ordering with domain and range
"""
import os
import json
from copy import deepcopy
import pandas as pd
from pandas.core.frame import DataFrame
from pandas.core.series import Series
from tqdm import tqdm
from settings import FOLDER_PATH


class Ordering:
    """
    Main ordering class for outgoing nodes

    (s, p, o)
    (p, rdf:domain, o2)
    (p, rdf:range, o3)

    (s, a , o2)
    (p, a, o3)

    ingoing -> filter on domain
    outgoing -> filter on range

    """
    def __init__(self, interface, domain_range: int = 1, focus_for_search: str = "event"):
        """
        - `interface`: permits to interact with the KG.
        Three implemented: HDTInterface, TriplInterface, SPARQLInterface
        BUT in practice, experiments with HDTInterface only
        --> TriplInterface and SPARQLInterface obsolete
        - `domain_range`: boolean, whether this parameter is activated or not
        - `focus_for_search`: type of classes searched during the traversal,
        e.g. `http://dbpedia.org/ontology/Event`
        """
        self.interface = interface

        self.dataset_type = interface.dataset_config["config_type"]
        self.prefix_entity = interface.dataset_config["prefix_entity"] \
            if "prefix_entity" in interface.dataset_config else None
        self.prefix_prop_direct = interface.dataset_config["prefix_constraint_direct"] \
            if "prefix_constraint_direct" in interface.dataset_config else None

        info_folder = os.path.join(FOLDER_PATH, "domain-range-pred")

        self.info = {}

        with open(os.path.join(info_folder, f"{self.dataset_type}-superclasses.json"),
                  "r", encoding="utf-8") as openfile:
            self.info["superclasses"] = json.load(openfile)

        with open(os.path.join(info_folder, f"{self.dataset_type}-domain.json"),
                  "r", encoding="utf-8") as openfile:
            self.info["domain"] = json.load(openfile)

        with open(os.path.join(info_folder, f"{self.dataset_type}-range.json"),
                  "r", encoding="utf-8") as openfile:
            self.info["range"] = json.load(openfile)

        self.type_node_to_pred = {
            "ingoing": "domain", "outgoing": "range"
        }

        self.focus_pred = [focus_for_search]
        self.domain_range = domain_range

    def __call__(self, triple_df: DataFrame,
                 type_node: str, info: dict[str, int],
                 iteration: int):
        """
        Params:
        - triple_df: pandas dataframe representing triples
        - type_node: type of triples of triple_df (`ingoing` or `outgoing`)
        - info: generic info updated in the ordering
        - iteration: iteration number
        Returns:
        - triple_df with superclass info
        - updated info
        """
        if type_node not in ['ingoing', 'outgoing']:
            raise ValueError("`type_node` should be either `ingoing` or `outgoing`")

        # 1. Removing literals from outgoing nodes
        # (Not expandable for search + Can create URI Too Long errors)
        # if type_node == 'outgoing':
        #     triple_df = self.remove_literals(triple_df=triple_df)
        # triple_df = self.remove_nodes(triple_df=triple_df, type_node=type_node)

        # 2. Superclasses
        if self.domain_range:  # retrieving info from domain_range
            # Fetching newly non encountered superclasses for
            # domain if ingoing, range if outgoing
            # self.add_superclass_to_class(df_pd=triple_df, type_node=type_node)

            # Adding superclass of domain/range predicate to the dataframe
            triple_df = self.add_superclass_to_df(triple_df=triple_df, type_node=type_node)

        else:  # adding empty superclass column
            triple_df["superclass"] = [[] for _ in range(triple_df.shape[0])]


        # 3. Updating info + Filtering out non relevant predicates
        return self.update_info_filter(triple_df=triple_df, type_node=type_node,
                                       info=info, iteration=iteration)

    def update_info_filter(self, triple_df: DataFrame,
                           type_node: str, info: dict[str, int], iteration: int):
        """
        1. Counting number of ingoing/outgoing edges,
        2. Counting number of triples with superclass info
        3. Counting number of triples with correct superclass info
        """
        if iteration not in info:
            info[iteration] = {
                "ingoing": 0,
                "ingoing_domain": 0,
                "ingoing_domain_relevant": 0,
                "outgoing": 0,
                "outgoing_range": 0,
                "outgoing_range_relevant": 0
            }

        triple_df.to_csv(f"{type_node}.csv")

        def filter_null(row: Series):
            return len(row.superclass) > 0

        info[iteration][f"{type_node}"] += triple_df.shape[0]
        info[iteration][f"{type_node}_{self.type_node_to_pred[type_node]}"] += \
            triple_df[triple_df.apply(filter_null, axis=1)].shape[0]

        def filter_func(row):
            return any(x in row.superclass for x in [""] + self.focus_pred)
        triple_df_filter = deepcopy(triple_df[triple_df.apply(filter_null, axis=1)])

        info[iteration][f"{type_node}_{self.type_node_to_pred[type_node]}_relevant"] += \
            triple_df_filter[triple_df_filter.apply(filter_func, axis=1)].shape[0]

        return triple_df, info


    def add_superclass_to_df(self, triple_df: DataFrame, type_node: str) \
        -> DataFrame:
        """ Adding col in df to add superclass of domain/range predicates"""

        def helper_func(x_input, lookup):
            res = []
            x_input = str(x_input).replace(self.prefix_prop_direct, self.prefix_entity)
            if x_input in lookup:
                for elt in [var for var in lookup[x_input] if var in self.info["superclasses"]]:
                    res += self.info["superclasses"][elt] + [elt]
            return res

        def get_superclass_func(lookup):
            if self.prefix_entity and self.prefix_prop_direct:
                return lambda x: helper_func(x, lookup)

            return lambda x: \
                    [y for elt in lookup[x] for y in self.info["superclasses"][elt]] if \
                        str(x) in lookup else []

        if type_node == "ingoing":  # self.info["domain"]
            triple_df["superclass"] = triple_df["predicate"].apply(
                get_superclass_func(lookup=self.info["domain"])
            )
        else:  # type_node == "outgoing" | self.info["range"]
            triple_df["superclass"] = triple_df["predicate"].apply(
                get_superclass_func(lookup=self.info["range"])
            )
        return triple_df

    @staticmethod
    def remove_literals(triple_df: DataFrame) -> DataFrame:
        """ Removing outgoing nodes that are Literals """
        triple_df = triple_df.fillna("")
        return triple_df[triple_df.object.str.startswith('http://')] \
            [["subject", "predicate", "object"]]

    def add_superclass_to_class(self, df_pd: DataFrame, type_node: str):
        """
        UPDATE (2022.07.11): obsolete, not used anymore
        Instead of searching domain/range/superclasses during the search,
        This is run before starting the search

        Input params:
        - df_pd: columns should be subject, predicate, object
            either ingoing or outgoing edges
        - type_node: types of nodes extracted, either
            `ingoing` (then interested in domain) or
            `outgoing` (then interested in range)

        Adds superclasses
        """
        domain_pred = "http://www.w3.org/2000/01/rdf-schema#domain"
        range_pred = "http://www.w3.org/2000/01/rdf-schema#range"

        if type_node == 'ingoing':
            filter_pred = [domain_pred]
        else:  # type_node == 'outgoing':
            filter_pred = [range_pred]

        preds = df_pd.predicate.unique()
        for i in tqdm(range(len(preds))):
            pred = preds[i]
            output = self.interface.run_request(
                params=dict(subject=str(pred)),
                filter_pred=filter_pred,
                filter_keep=True)

            for row in output:
                if str(row[1]) == domain_pred:
                    self.info["domain"][str(row[0])] = [str(row[2])]
                else:
                    self.info["range"][str(row[0])] = [str(row[2])]

                if row[2] not in self.info["superclasses"]:
                    self.info["superclasses"][str(row[2])] = \
                        self.interface.get_superclass(node=str(row[2]))


if __name__ == '__main__':
    from src.hdt_interface import HDTInterface

    folder = os.path.join(FOLDER_PATH, "src/tests/data")
    pending_ingoing_iter_1 = pd.read_csv(
        os.path.join(folder, "triply_ingoing_expected.csv")) \
            .fillna("")[["subject", "object", "predicate"]]

    print(pending_ingoing_iter_1)
    print(f"Init columns: {pending_ingoing_iter_1.columns}")

    ordering = Ordering(interface=HDTInterface())
    df_test, info_test = ordering(triple_df=pending_ingoing_iter_1,
                        type_node="ingoing", info={}, iteration=1)

    for _, row_test in df_test.iterrows():
        print(f"{row_test.predicate}\t {row_test.superclass}")

    print(info_test)
    print(f"Final columns: {df_test.columns}")
