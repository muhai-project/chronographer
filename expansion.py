import pandas as pd
from ranker import Ranker
from rdflib.term import URIRef
from sparql_interface import SPARQLInterface
from triply_interface import TriplInterface

class NodeExpansion:

    def __init__(self, rdf_type, iteration, type_ranking,
                 type_interface='triply'):
        if type_interface == 'triply':
            self.interface = TriplInterface()
        elif type_interface == 'sparql':
            self.interface = SPARQLInterface()
        else:
            raise ValueError("Not implemented")
        self.type_pred = URIRef("http://www.w3.org/1999/02/22-rdf-syntax-ns#type")
        self.rdf_type = rdf_type
        self.mapping = {uri: name for (name, uri) in rdf_type}
        self.iter = iteration

        self.ranker = Ranker(type_ranking=type_ranking)

    def _get_output_sparql(self, node, predicate):
        output_sparql = self.interface(node=node, 
                                       predicate=predicate)
        type_df = output_sparql[output_sparql.predicate == self.type_pred]
        path_df = output_sparql[output_sparql.predicate != self.type_pred]
        return type_df, path_df
    
    def _init_info_var(self):
        info = pd.DataFrame(columns=["path", "iteration"] + [x for elt in self.rdf_type for x in [f"# {elt[0]}", f"%-info-{elt[0]}"] ])
        return info
    
    def _get_info_from_type(self, type_df, path):

        type_df["type"] = type_df["object"].apply(lambda x: self.mapping[x] if x in self.mapping else "other")

        # grouped = type_df.groupby(["subject", "predicate", "type"]).agg({"object": "count"})
        grouped = type_df.groupby(["predicate", "type"]).agg({"object": "count"})
        # for _ in range(3):
        for _ in range(2):
            grouped.reset_index(level=0, inplace=True)
        # grouped["path"] =  [','.join([elt for elt in path] + [str(grouped.predicate.values[i]), str(grouped.subject.values[i])]) for i in range(grouped.shape[0])]
        grouped["path"] =  [','.join([elt for elt in path] + [str(grouped.predicate.values[i])]) for i in range(grouped.shape[0])]
        info = grouped.pivot(index="path", columns="type", values="object").fillna(0).astype(int)
        columns = info.columns
        info["tot"] = [sum([info[col].values[i] for col in columns]) for i in range(info.shape[0])]
        info["iteration"] = self.iter

        return info
    
    def _filter_sub_graph(self, type_df, path_df):
        # TO DO heuristics: check if needs update 
        to_keep = type_df[type_df.object.isin(list(self.mapping.keys()))].subject.values
        return path_df[path_df.subject.isin(to_keep)], path_df[~path_df.subject.isin(to_keep)]

    
    def __call__(self, args):
        # TO DO heuristics: check if needs update 

        # Updating path
        new_path = args["path"] + [args["node"]]

        # Querying sparql
        type_df, path_df = self._get_output_sparql(node=args["node"], predicate=args["predicate"])
        type_df_modified = pd.merge(type_df[["subject", "object"]], path_df[["subject", 'predicate']], how="left", on="subject")[["subject", "predicate", "object"]]
        
        # Interesting new paths
        info = self._get_info_from_type(type_df=type_df_modified, path=new_path)

        # Filter subgraph to keep
        subgraph, pending = self._filter_sub_graph(type_df=type_df, path_df=path_df)

        # Rank paths from remaining pending nodes
        ranked_paths = self.ranker(args=dict(df=pending, path=new_path))

        return ranked_paths, subgraph, pending, info 

        # type_df.to_csv("type_df.csv")
        # path_df.to_csv("path_df.csv")
        # info.to_csv("info.csv")
        # subgraph.to_csv("subgraph.csv")
        # pending.to_csv("pending.csv")

        


if __name__ == '__main__':
    node = "http://dbpedia.org/resource/Category:French_Revolution"
    predicate = list()
    rdf_type = [("event", URIRef("http://dbpedia.org/ontology/Event")),
                ("person", URIRef("http://dbpedia.org/ontology/Person"))]
    iteration = 2
    type_ranking = "entropy_predicate"

    node_expander = NodeExpansion(rdf_type=rdf_type, iteration=iteration, type_ranking=type_ranking)
    node_expander(args={"path": list(), "node": node, "predicate": predicate})
