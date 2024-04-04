# -*- codinf: utf-8 -*-
"""
Prompting
"""
import os
from urllib.parse import unquote
from tqdm import tqdm
import pandas as pd
import click
from variables import DBPEDIA_CONFIG, DATES, DBP_STR, CLASS_EVENT, PART_OF_MIL_CONF_PRED, \
    FR_IRI, COLUMNS, NS_DBR, PRED_FIlTER_LANG, PRED_RDF_TYPE, PREDICATES, \
            PROMPT_TRIPLES, END_PROMPT, ID_NODES, \
                arrange_df, write_triples, get_base_prompt
from src.hdt_interface import HDTInterface
from src.filtering import Filtering

FILTERING = Filtering(
    args={"when": 1,
          "point_in_time": DBPEDIA_CONFIG.get("point_in_time"),
          "start_dates": DBPEDIA_CONFIG.get("start_dates"),
          "end_dates": DBPEDIA_CONFIG.get("end_dates"),
          "places": None, "people": None,
          "dataset_type": "dbpedia"
    })
INTERFACE = HDTInterface()


def filter_data(triples):
    """ Filtering out some predicates + non-english content """
    triples = triples[~triples.predicate.isin(PREDICATES)]
    triples = triples[(~triples.predicate.isin(PRED_FIlTER_LANG)) | \
                      (triples.object.str.endswith('@en'))]
    triples = triples[~triples.predicate.str.startswith(DBP_STR)]
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
        


    triples = []
    for cand in filtered_cands:
        params = {"subject": cand}
        triples += INTERFACE.get_triples(**params)
    triples = pd.DataFrame(triples, columns=COLUMNS)
    date_df = triples[triples.predicate.isin(FILTERING.time["temporal"])]
    date_df.object = date_df.object.astype(str)
    date_df.object = date_df.object.apply(INTERFACE.pre_process_date)
    to_discard = FILTERING.get_to_discard_date(date_df=date_df, dates=info["dates"])
    triples = filter_data(triples[~triples.subject.isin(to_discard)])
    return triples


def get_sub_events_of_event_triples(**info):
    """ Get HDT triples for sub-events of events
    CONSTRUCT {
        ?se sem:subEventOf ?event ;
            ?p_se ?o_se .
        ?event ?p ?o .
    } WHERE {
        ?se dbo:isPartOfMilitaryConflict ?event ;
            ?p_se ?o_se .
        ?event ?p ?o .
        VALUES ?event {event}}
    """
    params = {"predicate": PART_OF_MIL_CONF_PRED, "object": info["event"]}
    events = pd.DataFrame(INTERFACE.get_triples(**params), columns=COLUMNS)
    triples = []
    for event in list(events.subject.unique()) + [info["event"]]:
        params = {"subject": event}
        triples += INTERFACE.get_triples(**params)
    triples = pd.DataFrame(triples, columns=COLUMNS)
    return filter_data(triples)


def get_actor_event_triples(**info):
    """ Get HDT triples for events in which actor participated
    CONSTRUCT {
        ?event sem:hasActor ?actor ;
           sem:hasBeginTimeStamp ?start ;
           sem:hasEndTimeStamp ?end ;
           ?p ?o .
    } WHERE {
        {
            ?event rdf:type dbo:Event ;
                   dbo:startDate ?start ;
                   dbo:endDate ?end ;
                   ?p ?o .
            ?event ?p1 ?actor .
        }
        UNION 
        {
            ?event rdf:type dbo:Event ;
                   dbo:startDate ?start ;
                   dbo:endDate ?end ;
                   ?p ?o .
            ?actor ?p2 ?event .
        }
        VALUES ?actor {actor}}
    """
    params = {"subject": info['actor']}
    triples = filter_data(pd.DataFrame(INTERFACE.get_triples(**params), columns=COLUMNS))
    candidates = list(triples.object.unique())
    params = {"object": info['actor']}
    triples = filter_data(pd.DataFrame(INTERFACE.get_triples(**params), columns=COLUMNS))
    candidates += list(triples.subject.unique())

    filtered_triples = pd.DataFrame(columns=COLUMNS)
    for cand in tqdm(candidates):
        params = {"subject": cand}
        outgoing = pd.DataFrame(INTERFACE.get_triples(**params), columns=COLUMNS)
        types = outgoing[outgoing.predicate == PRED_RDF_TYPE].object.unique()
        if CLASS_EVENT in types:
            date_df = outgoing[outgoing.predicate.isin(FILTERING.time["temporal"])]
            date_df.object = date_df.object.astype(str)
            date_df.object = date_df.object.apply(INTERFACE.pre_process_date)
            to_discard = FILTERING.get_to_discard_date(date_df=date_df, dates=DATES) + FILTERING.get_to_discard_regex(ingoing=outgoing, outgoing=pd.DataFrame(columns=COLUMNS), dates=DATES)
            if not to_discard:
                filtered_triples = pd.concat([filtered_triples, filter_data(outgoing[COLUMNS])])
    
    return filtered_triples


def get_actor_common_triples(**info):
    """ Events in which both actors participated """
    info_inter = {"actor": info['actor1']}
    triples_1 = get_actor_event_triples(**info_inter)
    info_inter = {"actor": info['actor2']}
    triples_2 = get_actor_event_triples(**info_inter)

    events_1 = triples_1[triples_1.subject != info['actor1']].subject.unique()
    events_2 = triples_2[triples_2.subject != info['actor2']].subject.unique()
    common_events = list(set(events_1).intersection(set(events_2)))

    return pd.concat([
        triples_1[triples_1.subject.isin([info['actor1']] + common_events)],
        triples_2[triples_2.subject.isin([info['actor2']] + common_events)]
    ]).drop_duplicates()


TYPE_PROMPT_TO_FUNC = {
    "summary": get_summary_triples,
    "cause_consequence": get_cause_consequence_triples,
    "event_type_timestamped": get_event_type_ts_triples,
    "sub_events_of_event": get_sub_events_of_event_triples,
    "actor_event": get_actor_event_triples, 
    "actor_common": get_actor_common_triples,
}


def get_triples_prompt(type_id, type_info, val):
    """ Get triples for context """
    prompt = get_base_prompt(type_id, type_info, val)

    if type_id == "event_type_timestamped":
        info = {"dates": list(val)}
    elif type_id == "actor_common":
        info = {"actor1": NS_DBR + unquote(val[0]), "actor2": NS_DBR + unquote(val[1])}
    elif type_id == "actor_event":
        info = {"actor": NS_DBR + unquote(val)}
    else:
        info = {"event": NS_DBR + unquote(val)}
    print(info)
    triples = TYPE_PROMPT_TO_FUNC[type_id](**info)
    df = arrange_df(df_input=triples)
    prompt = prompt + PROMPT_TRIPLES.replace("<TRIPLES>", write_triples(triples=df))
    return prompt

@click.command()
@click.argument("type_prompt")
@click.argument("save_folder")
def main(type_prompt, save_folder):
    """ Run and save all examples """
    type_prompts = ["triples"]
    if type_prompt not in type_prompts:
        raise ValueError(f"`type_prompt` must be within {type_prompts}")
    for type_id, info in ID_NODES.items():
        for type_info, vals in info.items():
            for val in vals:
                save_path = os.path.join(save_folder, f"{type_id}_{type_info}_{val}.txt")
                if not os.path.exists(save_path):
                    prompt = get_triples_prompt(type_id, type_info, val)
                    prompt += END_PROMPT
                    print(prompt)
                    print("======")
                    f = open(save_path, "w+", encoding="utf-8")
                    f.write(prompt)
                    f.close()


if __name__ == '__main__':
    main()
