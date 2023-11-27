# -*- coding: utf-8 -*-
"""
retrieve info from all generated NGs
"""
import os
import click
from tqdm import tqdm
from rdflib import Graph
import pandas as pd
from kglab.helpers.data_load import read_csv


COLUMNS_QUERY_FRAMES = ["event", "frame", "nb_frame"]
MAIN_QUERY_FRAMES = """
PREFIX ex: <http://example.com/> 
PREFIX nif: <http://persistence.uni-leipzig.org/nlp2rdf/ontologies/nif-core#>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX wsj: <https://w3id.org/framester/wsj/>
SELECT DISTINCT ?event ?frame (COUNT(DISTINCT(?annot)) as ?nb_frame) WHERE {
    ?event ex:abstract ?abstract .
    ?abstract nif:sentence ?sent .
    ?annot wsj:fromDocument ?sent ;
           rdf:type wsj:CorpusEntry ;
           wsj:onFrame ?frame ;
           wsj:withmappedrole ?role .
}
GROUP BY ?event ?frame
"""

COLUMNS_QUERY_ROLES = ["event", "annot", "frame", "lemma", "value", "fe", "ent"]
MAIN_QUERY_ROLES = """
PREFIX ex: <http://example.com/> 
PREFIX nif: <http://persistence.uni-leipzig.org/nlp2rdf/ontologies/nif-core#>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX wsj: <https://w3id.org/framester/wsj/>
PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
SELECT DISTINCT ?event ?annot ?frame ?lemma ?value ?fe ?ent WHERE {
    ?event ex:abstract ?abstract .
    ?abstract nif:sentence ?sent .
    ?annot wsj:fromDocument ?sent ;
           rdf:type wsj:CorpusEntry ;
           wsj:onFrame ?frame ;
           wsj:onLemma ?lemma ;
           wsj:withmappedrole ?role .
    ?role rdf:value ?value ;
          wsj:withfnfe ?fe .
    OPTIONAL {?role skos:related ?ent}
}
"""

COLUMNS_QUERY_CAUSATION = ["event", "annot", "sent_val", "frame", "lemma", "fe", "value", "ent"]
MAIN_QUERY_CAUSATION = """
PREFIX ex: <http://example.com/> 
PREFIX nif: <http://persistence.uni-leipzig.org/nlp2rdf/ontologies/nif-core#>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX wsj: <https://w3id.org/framester/wsj/>
PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
PREFIX frame: <https://w3id.org/framester/framenet/abox/frame/>
SELECT DISTINCT ?event ?annot ?sent_val ?frame ?lemma ?fe ?value ?ent WHERE {
    ?event ex:abstract ?abstract .
    ?abstract nif:sentence ?sent .
    ?sent rdf:value ?sent_val .
    ?annot wsj:fromDocument ?sent ;
           rdf:type wsj:CorpusEntry ;
           wsj:onFrame frame:Causation ;
           wsj:onLemma ?lemma ;
           wsj:withmappedrole ?role .
    ?role rdf:value ?value ;
          wsj:withfnfe ?fe .
    OPTIONAL {?role skos:related ?ent}
}
"""

def build_df(res, col):
    """ pandas df from rdflib graph query """
    return pd.DataFrame(
        [[row[x] for x in col] for row in res],
        columns=col)

def get_info_one_graph(graph, query, col):
    output = {}
    res = graph.query(query)
    df = build_df(res, col)
    return df

def generate_overleaf_text(data):
    text = f"""
    We generate narrative graphs from all the DBpedia events and sub-events that contain an abstract. In total, this sums up to {data['nb_event']} events. On average, each event had {data['avg_type_frame']} distinct frames and {data['avg_nb_frame']} instantiations of frames with at least one mapped role. The frames that appear the most in terms of number are: {data['top_frames_nb']}. The frames that appear the most across events are: {data['top_frames_event']}. The entities that appear the most across events are: {data['top_ent_event']}.

    We are more speficially interested about events and relations between events. For a qualitative analysis, we focus on causation frames, and manually annotate 100 frame instantiations on the frame elements Cause and Effect.
    """
    return text

def get_top(df_input, col, row_get_1, row_get_2):
    top_frames_nb = ["\\texttt{" + row[row_get_1].split('/')[-1] + "}" + " (" + '{:,}'.format(int(row[row_get_2])) + ")"  for _, row in df_input.sort_values(by=col, ascending=False).head(5).iterrows()]
    top_frames_nb[-1] = "and " + top_frames_nb[-1]
    top_frames_nb = [x.replace("_", "\\_") for x in top_frames_nb]
    return top_frames_nb

@click.command()
@click.option("--folder_input", help="(dbpedia) folder with subfolder of experiments")
@click.option("--folder_output", help="folder to save .csv files")
def main(folder_input: str, folder_output: str):
    """ Main """
    graph_all = Graph()
    exps = [os.path.join(folder_input, exp) for exp in os.listdir(folder_input) if not exp.endswith(".log")]
    nb_orig = len(exps)
    exps =  [x for x in exps if "frame_ng.ttl" in os.listdir(x)]
    nb_w_frame_ng = len(exps)
    print(f"{nb_w_frame_ng} events with frame KG ({round(100*nb_w_frame_ng/nb_orig, 1)}% of {nb_orig} events)")

    if not os.path.exists(os.path.join(folder_output, "df_frames.csv")):
        df_frames = pd.DataFrame(columns=COLUMNS_QUERY_FRAMES)
        df_roles = pd.DataFrame(columns=COLUMNS_QUERY_ROLES)
        df_causation = pd.DataFrame(columns=COLUMNS_QUERY_CAUSATION)
        for exp in tqdm(exps):
            graph = Graph()
            graph.parse(os.path.join(exp, "frame_ng.ttl"))
            graph_all.parse(os.path.join(exp, "frame_ng.ttl"))
            # Update frames
            curr_df = get_info_one_graph(graph, MAIN_QUERY_FRAMES, COLUMNS_QUERY_FRAMES)
            df_frames = pd.concat([df_frames, curr_df])
            # Update roles
            curr_df = get_info_one_graph(graph, MAIN_QUERY_ROLES, COLUMNS_QUERY_ROLES)
            df_roles = pd.concat([df_roles, curr_df])
            # Update causation
            curr_df = get_info_one_graph(graph, MAIN_QUERY_CAUSATION, COLUMNS_QUERY_CAUSATION)
            df_causation = pd.concat([df_causation, curr_df])
        
        df_frames.to_csv(os.path.join(folder_output, "df_frames.csv"))
        df_roles.to_csv(os.path.join(folder_output, "df_roles.csv"))
        df_causation.to_csv(os.path.join(folder_output, "df_causation.csv"))
    else:
        df_frames = read_csv(os.path.join(folder_output, "df_frames.csv"))
        df_roles = read_csv(os.path.join(folder_output, "df_roles.csv"))
        df_causation = read_csv(os.path.join(folder_output, "df_causation.csv"))
    
    mean_vals = df_frames.groupby("event").agg({"frame": "nunique", "nb_frame": "sum"}).mean()

    top_frames = df_frames.groupby("frame").agg({"nb_frame": "sum", "event": "nunique"}).reset_index()
    top_frames_nb = get_top(df_input=top_frames, col="nb_frame", row_get_2="nb_frame", row_get_1="frame")
    top_frames_event = get_top(df_input=top_frames, col="event", row_get_2="nb_frame", row_get_1="frame")

    top_entities = df_roles.groupby("ent").agg({"event": "nunique"}).reset_index()
    top_ent_event = get_top(df_input=top_entities, col="event", row_get_2="event", row_get_1="ent")

    data = {
        # '{:,}'.format()
        "nb_event": '{:,}'.format(df_frames.event.unique().shape[0]),
        "avg_type_frame": '{:,}'.format(round(mean_vals.frame)),
        "avg_nb_frame": '{:,}'.format(round(mean_vals.nb_frame)),
        "top_frames_nb": ", ".join(top_frames_nb),
        "top_frames_event": ", ".join(top_frames_event),
        "top_ent_event": ", ".join(top_ent_event)}
    text = generate_overleaf_text(data)
    print(text)
    f = open(os.path.join(folder_output, "overleaf.txt"), "w+")
    f.write(text)
    f.close()

    graph_all.serialize(os.path.join(folder_output, "graph_all.ttl"), format="ttl")
    

if __name__ == '__main__':
    main()