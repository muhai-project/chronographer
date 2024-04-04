# -*- codinf: utf-8 -*-
"""
Prompting
"""
import os
import click
from rdflib import Graph
from sparql_queries import QUERY_INFO_EVENT, QUERY_INFO_CAUSES_CONSEQUENCES, \
    QUERY_EVENT_TYPE_TIMESTAMPED, QUERY_SUB_EVENTS_OF_EVENT, QUERY_INFO_ACTOR, \
        QUERY_INTERACTION_ACTOR, QUERY_EVENT_FRAME
from variables import PROMPTS_EVENTS, PROMPTS_TS, PROMPTS_WHO, PROMPTS_INTERACTIONS, \
    PROMPT_TRIPLES, END_PROMPT, ID_NODES, \
        arrange_df, write_triples, get_base_prompt
from src.helpers import rdflib_to_pd
from kglab.helpers.kg_query import run_query
from kglab.helpers.variables import HEADERS_RDF_XML


SPARQL_ENDPOINT = "http://localhost:7200/repositories/2024-iswc-french-rev-frame-sem"



TYPE_PROMPT_TO_QUERY = {
    "summary": QUERY_INFO_EVENT,
    "cause_consequence": QUERY_INFO_CAUSES_CONSEQUENCES,
    "event_type_timestamped": QUERY_EVENT_TYPE_TIMESTAMPED,
    "sub_events_of_event": QUERY_SUB_EVENTS_OF_EVENT,
    "actor_event": QUERY_INFO_ACTOR, 
    "actor_common": QUERY_INTERACTION_ACTOR,
    "info_frame": QUERY_EVENT_FRAME
}


def get_query(type_id, val):
    """ SPARQL queries """
    query = TYPE_PROMPT_TO_QUERY[type_id]
    if type_id in PROMPTS_EVENTS:
        query = query.replace("<event>", val)
    if type_id in PROMPTS_TS:
        (start_date, end_date) = val
        query = query.replace("<start_date>", start_date) \
            .replace("<end_date>", end_date)
    if type_id in PROMPTS_WHO:
        query = query.replace("<actor>", val)
    if type_id in PROMPTS_INTERACTIONS:
        (actor1, actor2) = val
        query = query.replace("<actor1>", actor1) \
            .replace("<actor2>", actor2)
    return query


def get_triples_prompt(type_id, type_info, val):
    """ Get triples for context """
    prompt = get_base_prompt(type_id, type_info, val)
    graph = Graph()
    query = get_query(type_id, val)
    response = run_query(query=query,
                         sparql_endpoint=SPARQL_ENDPOINT,
                         headers=HEADERS_RDF_XML)
    graph.parse(data=response.text, format='xml')
    df = arrange_df(df_input=rdflib_to_pd(graph))
    prompt = prompt + PROMPT_TRIPLES.replace("<TRIPLES>", write_triples(triples=df))
    return prompt


@click.command()
@click.argument("type_prompt")
@click.argument("save_folder")
def main(type_prompt, save_folder):
    """ Run and save all examples """
    type_prompts = ["base", "triples"]
    if type_prompt not in type_prompts:
        raise ValueError(f"`type_prompt` must be within {type_prompts}")
    for type_id, info in ID_NODES.items():
        for type_info, vals in info.items():
            for val in vals:
                save_path = os.path.join(save_folder, f"{type_id}_{type_info}_{val}.txt")
                if not os.path.exists(save_path):
                    if type_prompt == "base":
                        prompt = get_base_prompt(type_id, type_info, val)
                    else:  # type_prompt == "triples":
                        prompt = get_triples_prompt(type_id, type_info, val)
                    prompt += END_PROMPT
                    print(prompt)
                    print("======")
                    f = open(save_path, "w+", encoding="utf-8")
                    f.write(prompt)
                    f.close()


if __name__ == '__main__':
    main()
