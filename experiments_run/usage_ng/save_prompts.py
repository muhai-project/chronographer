# -*- codinf: utf-8 -*-
"""
Prompting
"""
import os
import click
import requests
from urllib.parse import unquote
from rdflib import Graph
from sparql_queries import QUERY_INFO_EVENT, QUERY_INFO_CAUSES_CONSEQUENCES, \
    QUERY_EVENT_TYPE_TIMESTAMPED, QUERY_SUB_EVENTS_OF_EVENT
from src.helpers import rdflib_to_pd
from kglab.helpers.kg_query import run_query
from kglab.helpers.variables import HEADERS_RDF_XML, HEADERS_CSV

SPARQL_ENDPOINT = "http://localhost:7200/repositories/2024-iswc-french-rev-frame-sem"

EVENT = "French Revolution"
PROMPTS_EVENTS = {
    "summary": {
        "event": f"Please provide a summary of the {EVENT}.",
        "sub_event": f"Please provide a summary of the <sub_event> during the {EVENT}."
    },
    "cause_consequence": {
        "event": f"What happened at the end of the {EVENT}? Please provide the main causal happenings that led to the unfolding of the {EVENT}.",
        "sub_event": f"What happened at the end of the <sub_event> during the {EVENT}? Please provide the main causal happenings that led to the unfolding of the <sub_event>."
    },
    "sub_events_of_event": {
        "event": f"What were the main events of the <event> during the {EVENT}? Can you order them in chronological order?"
    }
}

PROMPTS_TS = {
    "event_type_timestamped": "What were the main type of events that happened between <start_date> and <end_date>?"
}

PROMPT_TRIPLES = """ You should utilise relevant content and the following context triples.

Context triples:
```triples
<TRIPLES>
```
"""

TYPE_PROMPT_TO_QUERY = {
    "summary": QUERY_INFO_EVENT,
    "cause_consequence": QUERY_INFO_CAUSES_CONSEQUENCES,
    "event_type_timestamped": QUERY_EVENT_TYPE_TIMESTAMPED,
    "sub_events_of_event": QUERY_SUB_EVENTS_OF_EVENT
}

ID_NODES = {
    # "summary": {
    #     "event": ["French_Revolution"],
    #     "sub_event": ["Storming_of_the_Bastille", "Flanders_campaign", "Infernal_columns"]
    # },
    # "cause_consequence": {
    #     "event": ["French_Revolution"],
    #     "sub_event": ["Action_of_19_December_1796", "Battle_of_Cassano", "Battle_of_Winterthur"]
    # },
    # "event_type_timestamped": {
    #     "periods": [("1789-01-01", "1790-01-01"), ("1792-01-01", "1793-01-01")]
    # },
    "sub_events_of_event": {
        "event": ["War_of_the_Second_Coalition", "French_Revolutionary_Wars"]
    }
}

def arrange_df(df_input):
    for col in ["subject", "predicate", "object"]:
        df_input[col] = df_input[col].apply(lambda x: unquote(x).split("/")[-1])
    return df_input

def write_triples(triples):
    """ Add triples to prompt """
    res = []
    for _, row in triples.iterrows():
        res.append(f"({row.subject}, {row.predicate}, {row.object})")
    return "\n".join(res)

def get_base_prompt(type_id, type_info, val):
    if type_id in PROMPTS_EVENTS:
        prompt = PROMPTS_EVENTS[type_id][type_info]
        if "<" in prompt:
            prompt = prompt.replace(f"<{type_info}>", val.replace("_", " "))
    if type_id in PROMPTS_TS:
        prompt = PROMPTS_TS[type_id]
        (start_date, end_date) = val
        prompt = prompt.replace("<start_date>", start_date) \
            .replace("<end_date>", end_date)
    return prompt

def get_query(type_id, val):
    query = TYPE_PROMPT_TO_QUERY[type_id]
    if type_id in PROMPTS_EVENTS:
        query = query.replace("<event>", val)
    if type_id in PROMPTS_TS:
        (start_date, end_date) = val
        query = query.replace("<start_date>", start_date) \
            .replace("<end_date>", end_date)
    return query

def get_triples_prompt(type_id, type_info, val):
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
                    print(prompt)
                    print("======")
                    # f = open(save_path, "w+")
                    # f.write(prompt)
                    # f.close()
                

    


if __name__ == '__main__':
    main()
