# -*- codinf: utf-8 -*-
"""
Prompting
"""
import os
from urllib.parse import unquote
import click
from rdflib import Graph
from sparql_queries import QUERY_INFO_EVENT, QUERY_INFO_CAUSES_CONSEQUENCES, \
    QUERY_EVENT_TYPE_TIMESTAMPED, QUERY_SUB_EVENTS_OF_EVENT, QUERY_INFO_ACTOR, \
        QUERY_INTERACTION_ACTOR, QUERY_EVENT_FRAME
from src.helpers import rdflib_to_pd
from kglab.helpers.kg_query import run_query
from kglab.helpers.variables import HEADERS_RDF_XML

SPARQL_ENDPOINT = "http://localhost:7200/repositories/2024-iswc-french-rev-frame-sem"

EVENT = "French Revolution"
PROMPTS_EVENTS = {
    "summary": {
        "event": f"Please provide a summary of the {EVENT}.",
        "sub_event": f"Please provide a summary of the <sub_event> during the {EVENT}."
    },
    "cause_consequence": {
        "event": f"What happened at the end of the {EVENT}? " + \
            f"Please provide the main causal happenings that led to the unfolding of the {EVENT}.",
        "sub_event": f"What happened at the end of the <sub_event> during the {EVENT}? " + \
            "Please provide the main causal happenings that led to the unfolding of the <sub_event>."
    },
    "sub_events_of_event": {
        "event": f"What were the main events of the <event> during the {EVENT}? " + \
            "Can you list and order them in chronological order?"
    },
    "info_frame": {
        "event": f"Which conceptual frames were evoked during the <event> during the {EVENT}?"
    }
}

PROMPTS_TS = {
    "event_type_timestamped": "What were the main type of events that happened " + \
        f"between <start_date> and <end_date> during the {EVENT}?"
}

PROMPTS_WHO = {
    "actor_event": f"What happened to <actor> during the {EVENT}?"
}

PROMPTS_INTERACTIONS = {
    "actor_common": f"In which events were <actor1> and <actor2> both involved during the {EVENT}?"
}

PROMPT_TRIPLES = """ You should utilise relevant content and the following context triples.

Context triples:
```triples
<TRIPLES>
```
"""

END_PROMPT = "Be concise in your answer."

TYPE_PROMPT_TO_QUERY = {
    "summary": QUERY_INFO_EVENT,
    "cause_consequence": QUERY_INFO_CAUSES_CONSEQUENCES,
    "event_type_timestamped": QUERY_EVENT_TYPE_TIMESTAMPED,
    "sub_events_of_event": QUERY_SUB_EVENTS_OF_EVENT,
    "actor_event": QUERY_INFO_ACTOR, 
    "actor_common": QUERY_INTERACTION_ACTOR,
    "info_frame": QUERY_EVENT_FRAME
}

ID_NODES = {
    "summary": {
        "event": ["French_Revolution"],
        "sub_event": ["Storming_of_the_Bastille", "Flanders_campaign", "Infernal_columns"]
    },
    "cause_consequence": {
        "event": ["French_Revolution"],
        "sub_event": ["Action_of_19_December_1796", "Battle_of_Winterthur"]
    },
    "event_type_timestamped": {
        "periods": [("1789-01-01", "1790-01-01"), ("1792-01-01", "1793-01-01")]
    },
    "sub_events_of_event": {
        "event": ["War_of_the_Second_Coalition", "French_Revolutionary_Wars"]
    },
    "actor_event": {
        "actor": ["Napoleon", "Paul_Barras", "Juan_Nepomuceno_de_Quesada",
                  "Jean_Moulston", "William_Lumley", "Antoine_Balland",
                  "Jacques_Gilles_Henri_Goguet"]
    },
    "actor_common": {
        "actor": [("Napoleon", "Paul_Barras"), ("French_First_Republic", "Dutch_Republic"),
                  ("Jean-Baptiste_Jourdan", "Joseph_Bonaparte"),
                  ("Charles_IV_of_Spain", "Francis_II%2C_Holy_Roman_Emperor"),
                  ("Guillaume_Brune", "Magnus_Gustav_von_Essen")]
    },
    "info_frame": {
        "event": ["Storming_of_the_Bastille", "Coup_of_18_Brumaire"]
    }
}

def arrange_df(df_input):
    """ More readable triples """
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
    """ Base prompt """
    if type_id in PROMPTS_EVENTS:
        prompt = PROMPTS_EVENTS[type_id][type_info]
        if "<" in prompt:
            prompt = prompt.replace(f"<{type_info}>", val.replace("_", " "))
    if type_id in PROMPTS_TS:
        prompt = PROMPTS_TS[type_id]
        (start_date, end_date) = val
        prompt = prompt.replace("<start_date>", start_date) \
            .replace("<end_date>", end_date)
    if type_id in PROMPTS_WHO:
        prompt = PROMPTS_WHO[type_id]
        prompt = prompt.replace("<actor>", val)
    if type_id in PROMPTS_INTERACTIONS:
        prompt = PROMPTS_INTERACTIONS[type_id]
        (actor1, actor2) = val
        prompt = prompt.replace("<actor1>", actor1) \
            .replace("<actor2>", actor2)
    return prompt

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
                    print(prompt)
                    print("======")
                    f = open(save_path, "w+", encoding="utf-8")
                    f.write(prompt)
                    f.close()


if __name__ == '__main__':
    main()
