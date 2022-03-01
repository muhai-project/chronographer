"""
Filtering class: preprocessing outgoing nodes retrieved
(e.g. we only want to keep nodes with a URI, and not literals)
"""

from tqdm import tqdm

import pandas as pd
from src.triply_interface import TriplInterface


class Filtering:
    """
    Main filtering class for outgoing nodes

    (s, p, o)
    (p, rdf:domain, o2)
    (p, rdf:range, o3)

    (s, a , o2)
    (p, a, o3)

    ingoing -> filter on domain
    outgoing -> filter on range

    """
    def __init__(self, focus: str = "event"):
        self.domain_pred = "http://www.w3.org/2000/01/rdf-schema#domain"
        self.range_pred = "http://www.w3.org/2000/01/rdf-schema#range"
        self.interface = TriplInterface(default_pred=[])

        self.superclasses = {}
        self.domain = {}
        self.range = {}

        self.type_node_to_pred = {
            "ingoing": "domain", "outgoing": "range"
        }

        self.focus_to_pred = {
            "event": "http://dbpedia.org/ontology/Event"
        }
        self.focus_pred = self.focus_to_pred[focus]
        self.discard_nodes = ["http://dbpedia.org/resource/Category:"]

    def __call__(self, triple_df: pd.core.frame.DataFrame,
                 type_node: str, info: dict[str, int],
                 iteration: int):
        """
        Params:
        - triple_df: pandas dataframe representing triples
        - type_node: type of triples of triple_df (`ingoing` or `outgoing`)
        - info: generic info updated in the filtering
        - iteration: iteration number
        Returns:
        - triple_df with superclass info
        - updated info
        """
        if type_node not in ['ingoing', 'outgoing']:
            raise ValueError("`type_node` should be either `ingoing` or `outgoing`")

        # 1. Removing literals from outgoing nodes
        # (Not expandable for search + Can create URI Too Long errors)
        if type_node == 'outgoing':
            triple_df = self.remove_literals(triple_df=triple_df)
        triple_df = self.remove_nodes(triple_df=triple_df, type_node=type_node)

        # 2. Superclasses
        # Fetching newly non encountered superclasses for
        # domain if ingoing, range if outgoing
        self.add_superclass_to_class(df_pd=triple_df, type_node=type_node)

        # Adding superclass of domain/range predicate to the dataframe
        triple_df = self.add_superclass_to_df(triple_df=triple_df, type_node=type_node)

        # 3. Updating info + Filtering out non relevant predicates
        return self.update_info_filter(triple_df=triple_df, type_node=type_node,
                                       info=info, iteration=iteration)

    def update_info_filter(self, triple_df: pd.core.frame.DataFrame,
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

        info[iteration][f"{type_node}"] += triple_df.shape[0]
        info[iteration][f"{type_node}_{self.type_node_to_pred[type_node]}"] += \
            triple_df[triple_df.superclass != ""].shape[0]

        triple_df = triple_df[triple_df.superclass.isin(["", self.focus_pred])]

        info[iteration][f"{type_node}_{self.type_node_to_pred[type_node]}_relevant"] += \
            triple_df[triple_df.superclass != ""].shape[0]

        return triple_df, info


    def add_superclass_to_df(self, triple_df, type_node):
        """ Adding col in df to add superclass of domain/range predicaten"""
        if type_node == "ingoing":  # self.domain
            triple_df["superclass"] = triple_df["predicate"].apply(
                lambda x: str(self.superclasses[self.domain[str(x)]]) if \
                    str(x) in self.domain else ""
            )
        else:  # type_node == "outgoing" | self.range
            triple_df["superclass"] = triple_df["predicate"].apply(
                lambda x: str(self.superclasses[self.range[str(x)]]) if \
                    str(x) in self.range else ""
            )
        return triple_df

    @staticmethod
    def remove_literals(triple_df):
        """ Removing outgoing nodes that are Literals """
        triple_df = triple_df.fillna("")
        return triple_df[triple_df.object.str.startswith('http://')] \
            [["subject", "predicate", "object"]]

    def remove_nodes(self, triple_df, type_node):
        """ Filtering out certain nodes """
        triple_df = triple_df.fillna("")
        col = "subject" if type_node == "ingoing" else "object"
        triple_df['filter'] = str(triple_df[col])
        for node in self.discard_nodes:
            triple_df = triple_df[~triple_df[col].str.startswith(node)]
        return triple_df[["subject", "predicate", "object"]]

    def add_superclass_to_class(self, df_pd: pd.core.frame.DataFrame, type_node: str):
        """
        Input params:
        - df_pd: columns should be subject, predicate, object
            either ingoing or outgoing edges
        - type_node: types of nodes extracted, either
            `ingoing` (then interested in domain) or
            `outgoing` (then interested in range)

        Adds superclasses
        """
        if type_node == 'ingoing':
            filter_pred = [self.domain_pred]
        else:  # type_node == 'outgoing':
            filter_pred = [self.range_pred]

        preds = df_pd.predicate.unique()
        for i in tqdm(range(len(preds))):
            pred = preds[i]
            output = self.interface.run_curl_request(
                params=dict(subject=str(pred)),
                filter_pred=filter_pred,
                filter_keep=True)

            for row in output:
                if str(row[1]) == self.domain_pred:
                    self.domain[str(row[0])] = str(row[2])
                else:
                    self.range[str(row[0])] = str(row[2])

                if row[2] not in self.superclasses:
                    self.superclasses[str(row[2])] = \
                        self.interface.get_superclass(node=str(row[2]))


if __name__ == '__main__':
    import os
    from settings import FOLDER_PATH

    folder = os.path.join(FOLDER_PATH, "src/tests")
    pending_ingoing_iter_1 = pd.read_csv(
        os.path.join(folder, "triply_ingoing_expected.csv")) \
            .fillna("")[["subject", "object", "predicate"]]

    filtering = Filtering()
    df_test, info_test = filtering(triple_df=pending_ingoing_iter_1,
                        type_node="ingoing", info={}, iteration=1)

    for _, row_test in df_test.iterrows():
        print(f"{row_test.predicate}\t {row_test.superclass}")

    print(info_test)
