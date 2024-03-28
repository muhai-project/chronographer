# -*- codinf: utf-8 -*-
"""
Prompting
"""
import os
import pandas as pd
from urllib.parse import unquote
import click
from rdflib import Graph
from sparql_queries import QUERY_INFO_EVENT, QUERY_INFO_CAUSES_CONSEQUENCES, \
    QUERY_EVENT_TYPE_TIMESTAMPED, QUERY_SUB_EVENTS_OF_EVENT, QUERY_INFO_ACTOR, \
        QUERY_INTERACTION_ACTOR, QUERY_EVENT_FRAME
from src.filtering import Filtering
from src.helpers import rdflib_to_pd
from src.hdt_interface import HDTInterface
from kglab.helpers.kg_query import run_query
from kglab.helpers.variables import HEADERS_RDF_XML, NS_DBR

INTERFACE = HDTInterface()
NS_DBR = str(NS_DBR)
CLASS_EVENT = "http://dbpedia.org/ontology/Event" 
FR_IRI = "http://dbpedia.org/resource/French_Revolution"
COLUMNS = ["subject", "predicate", "object"]
NS_DBO = "http://dbpedia.org/ontology/"
PRED_FIlTER_LANG = ["http://dbpedia.org/ontology/abstract",
                    "http://www.w3.org/2000/01/rdf-schema#comment",
                    "http://www.w3.org/2000/01/rdf-schema#label"]
PRED_PART_OF = "http://dbpedia.org/ontology/isPartOfMilitaryConflict"
PRED_RDF_TYPE = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
PREDICATES = [
    'http://www.w3.org/2000/01/rdf-schema#seeAlso',
    'http://dbpedia.org/ontology/series',
    'http://dbpedia.org/ontology/nonFictionSubject',
    'http://purl.org/dc/elements/1.1/subject',
    'http://dbpedia.org/ontology/wikiPageWikiLink',
    'http://dbpedia.org/ontology/wikiPageRedirects',
    'http://dbpedia.org/ontology/wikiPageDisambiguates',
    'http://dbpedia.org/property/wikiPageUsesTemplate',
    "http://xmlns.com/foaf/0.1/depiction",
    "http://xmlns.com/foaf/0.1/isPrimaryTopicOf",
    "http://dbpedia.org/ontology/thumbnail",
    "http://dbpedia.org/ontology/wikiPageExternalLink",
    "http://dbpedia.org/ontology/wikiPageID",
    "http://dbpedia.org/ontology/wikiPageLength",
    "http://dbpedia.org/ontology/wikiPageRevisionID",
    "http://www.w3.org/2002/07/owl#sameAs",
    "http://www.w3.org/ns/prov#wasDerivedFrom",
    "http://dbpedia.org/ontology/wikiPageWikiLinkText",
    "http://dbpedia.org/ontology/wikiPageOutDegree",
    "http://dbpedia.org/property/imageCaption",
    "http://dbpedia.org/property/imageName",
    "http://dbpedia.org/property/onlinebooks",
    "http://www.w3.org/2004/02/skos/core#exactMatch",
    "http://purl.org/dc/terms/subject",
    "http://dbpedia.org/ontology/soundRecording",
    "http://dbpedia.org/property/international",
    "http://dbpedia.org/property/setting",
    "http://dbpedia.org/property/subject",
    "http://xmlns.com/foaf/0.1/primaryTopic",
    "http://dbpedia.org/ontology/internationalAffiliation"
]
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
        f"between <start_date> and <end_date>? during the {EVENT}"
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

def filter_data(triples):
    triples = triples[~triples.predicate.isin(PREDICATES)]
    triples = triples[(~triples.predicate.isin(PRED_FIlTER_LANG)) | (triples.object.str.endswith('@en'))]
    return triples

def get_summary_triples(**info):
    """ Get HDT triples for summary questions
    SPARQL equivalent: 
    CONSTRUCT {
        ?event ?p1 ?o
        ?s ?p2 ?event
    } WHERE {
        {?event ?p1 ?o}
        UNION
        {?s ?p2 ?event}
        VALUES ?event {event}}"""
    params = {"subject": info["event"]}
    triples = INTERFACE.get_triples(**params)
    params = {"object": info["event"]}
    triples += INTERFACE.get_triples(**params)
    triples = pd.DataFrame(triples, columns=COLUMNS)
    triples = filter_data(triples=triples)
    return triples

def get_cause_consequence_triples(**info):
    """ Get HDT triples for cause-consequence questions
    SPARQL equivalent: 
    CONSTRUCT {
        ?event ?p1 ?o
        ?s ?p2 ?event
    } WHERE {
        {?event ?p1 ?o}
        UNION
        {?s ?p2 ?event}
        VALUES ?event {event}}"""
    return get_summary_triples(**info)

def get_event_type_ts_triples(**info):
    """ Get HDT triples for event type timestamped questions
    SPARQL equivalent: 
    CONSTRUCT {
        ?event dbo:abstract ?abstract ;
               dbo:startDate ?begin_ts ;
               dbo:endDate ?end_ts ;
               rdf:type ?type .
    } WHERE {
        {?event ?p1 dbr:French_Revolution ;
               rdf:type dbo:Event, ?type ;
               dbo:startDate ?begin_ts ;
               dbo:endDate ?end_ts .}
        UNION
        {
         dbr:French_Revolution ?p2 ?event .
         ?event rdf:type dbo:Event, ?type ;
                dbo:startDate ?begin_ts ;
                dbo:endDate ?end_ts .
        }
        FILTER(((?begin_ts >= "<start_date>T00:00:00"^^xsd:dateTime) && 
                (?begin_ts <= "<end_date>T00:00:00"^^xsd:dateTime)) ||
               ((?end_ts >= "<start_date>T00:00:00"^^xsd:dateTime) &&
                (?end_ts <= "<end_date>T00:00:00"^^xsd:dateTime)))
        VALUES ?event {event}}"""
    params = {"object": FR_IRI}
    candidates = INTERFACE.get_triples(**params)
    params = {"subject": FR_IRI}
    candidates += INTERFACE.get_triples(**params)
    data = filter_data(pd.DataFrame(candidates, columns=COLUMNS))
    candidates = data.subject.unique()

    filtered_cands = []
    for cand in candidates:
        params = {"subject": cand, "predicate": PRED_RDF_TYPE}
        types = [x[2] for x in INTERFACE.get_triples(**params)]
        if CLASS_EVENT in types:
            filtered_cands.append(cand)
    print(filtered_cands)


TYPE_PROMPT_TO_FUNC = {
    "summary": get_summary_triples,
    "cause_consequence": get_cause_consequence_triples,
    # "event_type_timestamped": QUERY_EVENT_TYPE_TIMESTAMPED,
    # "sub_events_of_event": QUERY_SUB_EVENTS_OF_EVENT,
    # "actor_event": QUERY_INFO_ACTOR, 
    # "actor_common": QUERY_INTERACTION_ACTOR,
    # "info_frame": QUERY_EVENT_FRAME
}

# ID_NODES = {
#     "summary": {
#         "event": ["French_Revolution"],
#         "sub_event": ["Storming_of_the_Bastille", "Flanders_campaign", "Infernal_columns"]
#     },
#     "cause_consequence": {
#         "event": ["French_Revolution"],
#         "sub_event": ["Action_of_19_December_1796", "Battle_of_Winterthur"]
#     },
#     "event_type_timestamped": {
#         "periods": [("1789-01-01", "1790-01-01"), ("1792-01-01", "1793-01-01")]
#     },
#     "sub_events_of_event": {
#         "event": ["War_of_the_Second_Coalition", "French_Revolutionary_Wars"]
#     },
#     "actor_event": {
#         "actor": ["Napoleon", "Paul_Barras", "Juan_Nepomuceno_de_Quesada",
#                   "Jean_Moulston", "William_Lumley", "Antoine_Balland",
#                   "Jacques_Gilles_Henri_Goguet"]
#     },
#     "actor_common": {
#         "actor": [("Napoleon", "Paul_Barras"), ("French_First_Republic", "Dutch_Republic"),
#                   ("Jean-Baptiste_Jourdan", "Joseph_Bonaparte"),
#                   ("Charles_IV_of_Spain", "Francis_II%2C_Holy_Roman_Emperor"),
#                   ("Guillaume_Brune", "Magnus_Gustav_von_Essen")]
#     },
#     "info_frame": {
#         "event": ["Storming_of_the_Bastille", "Coup_of_18_Brumaire"]
#     }
# }

# def arrange_df(df_input):
#     """ More readable triples """
#     for col in ["subject", "predicate", "object"]:
#         df_input[col] = df_input[col].apply(lambda x: unquote(x).split("/")[-1])
#     return df_input

# def write_triples(triples):
#     """ Add triples to prompt """
#     res = []
#     for _, row in triples.iterrows():
#         res.append(f"({row.subject}, {row.predicate}, {row.object})")
#     return "\n".join(res)

# def get_base_prompt(type_id, type_info, val):
#     """ Base prompt """
#     if type_id in PROMPTS_EVENTS:
#         prompt = PROMPTS_EVENTS[type_id][type_info]
#         if "<" in prompt:
#             prompt = prompt.replace(f"<{type_info}>", val.replace("_", " "))
#     if type_id in PROMPTS_TS:
#         prompt = PROMPTS_TS[type_id]
#         (start_date, end_date) = val
#         prompt = prompt.replace("<start_date>", start_date) \
#             .replace("<end_date>", end_date)
#     if type_id in PROMPTS_WHO:
#         prompt = PROMPTS_WHO[type_id]
#         prompt = prompt.replace("<actor>", val)
#     if type_id in PROMPTS_INTERACTIONS:
#         prompt = PROMPTS_INTERACTIONS[type_id]
#         (actor1, actor2) = val
#         prompt = prompt.replace("<actor1>", actor1) \
#             .replace("<actor2>", actor2)
#     return prompt

# def get_query(type_id, val):
#     """ SPARQL queries """
#     query = TYPE_PROMPT_TO_QUERY[type_id]
#     if type_id in PROMPTS_EVENTS:
#         query = query.replace("<event>", val)
#     if type_id in PROMPTS_TS:
#         (start_date, end_date) = val
#         query = query.replace("<start_date>", start_date) \
#             .replace("<end_date>", end_date)
#     if type_id in PROMPTS_WHO:
#         query = query.replace("<actor>", val)
#     if type_id in PROMPTS_INTERACTIONS:
#         (actor1, actor2) = val
#         query = query.replace("<actor1>", actor1) \
#             .replace("<actor2>", actor2)
#     return query

# def get_triples_prompt(type_id, type_info, val):
#     """ Get triples for context """
#     prompt = get_base_prompt(type_id, type_info, val)
#     graph = Graph()
#     query = get_query(type_id, val)
#     response = run_query(query=query,
#                          sparql_endpoint=SPARQL_ENDPOINT,
#                          headers=HEADERS_RDF_XML)
#     graph.parse(data=response.text, format='xml')
#     df = arrange_df(df_input=rdflib_to_pd(graph))
#     prompt = prompt + PROMPT_TRIPLES.replace("<TRIPLES>", write_triples(triples=df))
#     return prompt

# @click.command()
# @click.argument("type_prompt")
# @click.argument("save_folder")
# def main(type_prompt, save_folder):
#     """ Run and save all examples """
#     type_prompts = ["base", "triples"]
#     if type_prompt not in type_prompts:
#         raise ValueError(f"`type_prompt` must be within {type_prompts}")
#     for type_id, info in ID_NODES.items():
#         for type_info, vals in info.items():
#             for val in vals:
#                 save_path = os.path.join(save_folder, f"{type_id}_{type_info}_{val}.txt")
#                 if not os.path.exists(save_path):
#                     if type_prompt == "base":
#                         prompt = get_base_prompt(type_id, type_info, val)
#                     else:  # type_prompt == "triples":
#                         prompt = get_triples_prompt(type_id, type_info, val)
#                     print(prompt)
#                     print("======")
#                     f = open(save_path, "w+", encoding="utf-8")
#                     f.write(prompt)
#                     f.close()


if __name__ == '__main__':
    # main()
    info = {}
    get_event_type_ts_triples(**info)


