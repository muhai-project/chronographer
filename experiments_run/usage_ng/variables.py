# -*- codinf: utf-8 -*-
"""
Common variables used for saving prompts
"""
import os
from urllib.parse import unquote
import yaml
from settings import FOLDER_PATH

## All
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

END_PROMPT = """
Be concise in your answer.
"""

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
}

######################################

## Functions
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
        prompt = prompt.replace("<actor>", unquote(val.replace("_", " ")))
    if type_id in PROMPTS_INTERACTIONS:
        prompt = PROMPTS_INTERACTIONS[type_id]
        (actor1, actor2) = val
        prompt = prompt.replace("<actor1>", unquote(actor1.replace("_", " "))) \
            .replace("<actor2>", unquote(actor2.replace("_", " ")))
    return prompt
######################################


## DBpedia Only
with open(os.path.join(FOLDER_PATH,"dataset-config", "dbpedia.yaml"),
          encoding='utf-8') as file:
    DBPEDIA_CONFIG = yaml.load(file, Loader=yaml.FullLoader)

DATES = ["1789-05-05", "1799-12-31"]

DBP_STR = "http://dbpedia.org/property/"
CLASS_EVENT = "http://dbpedia.org/ontology/Event" 
PART_OF_MIL_CONF_PRED = "http://dbpedia.org/ontology/isPartOfMilitaryConflict" 
FR_IRI = "http://dbpedia.org/resource/French_Revolution"
COLUMNS = ["subject", "predicate", "object"]
NS_DBO = "http://dbpedia.org/ontology/"
NS_DBR = "http://dbpedia.org/resource/"
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
    "http://dbpedia.org/ontology/internationalAffiliation",
    'http://dbpedia.org/property/caption',
    'http://dbpedia.org/property/imageSize',
    'http://dbpedia.org/ontology/notes',
    PRED_RDF_TYPE,
    'http://schema.org/sameAs',
    'http://www.w3.org/2000/01/rdf-schema#comment',
    'http://xmlns.com/foaf/0.1/name',
    'http://www.georss.org/georss/point',
    'http://www.w3.org/2003/01/geo/wgs84_pos#lat',
    'http://www.w3.org/2003/01/geo/wgs84_pos#long',
    'http://xmlns.com/foaf/0.1/homepage',
    'http://dbpedia.org/ontology/wikiPageInterLanguageLink',
    'http://www.w3.org/2002/07/owl#differentFrom'
]
######################################