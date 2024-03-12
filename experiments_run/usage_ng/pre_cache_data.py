# -*- coding: utf-8 -*-
""" 
Pre-caching data for better usage
"""
import os
from tqdm import tqdm
from urllib.parse import unquote
from geopy.geocoders import Nominatim
from settings import FOLDER_PATH
from kglab.helpers.kg_query import run_query
from kglab.helpers.variables import HEADERS_CSV

PREFIXES = """
PREFIX dbo: <http://dbpedia.org/ontology/>
PREFIX ex: <http://example.com/>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX dbr: <http://dbpedia.org/resource/> 
PREFIX frame: <https://w3id.org/framester/framenet/abox/frame/> 
PREFIX wsj: <https://w3id.org/framester/wsj/> 
PREFIX nif: <http://persistence.uni-leipzig.org/nlp2rdf/ontologies/nif-core#> 
PREFIX sem: <http://semanticweb.cs.vu.nl/2009/11/sem/> 
PREFIX skos: <http://www.w3.org/2004/02/skos/core#> 
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
PREFIX gfe: <https://w3id.org/framester/framenet/abox/gfe/>
"""
START, END = "{", "}"
SPARQL_ENDPOINT = "http://localhost:7200/repositories/2024-iswc-french-rev-frame-sem"
SPARQL_ENDPOINT_FS = "http://localhost:7200/repositories/framester-4-0-0"
GEOLOCATOR = Nominatim(user_agent="test")

QUERY_LOCATION_SEM = PREFIXES + \
    """
        SELECT ?event ?location WHERE {
            ?event rdf:type sem:Event ;
                sem:hasPlace ?location .
        }
    """

QUERY_LOCATION_SEM_FRAME = PREFIXES + \
    """
        SELECT ?event ?location WHERE {
        ?event ex:abstract ?abstract .
        ?abstract rdf:value ?abstract_label ;
                nif:sentence ?sentence .
        ?annotation rdf:type wsj:CorpusEntry ;
                    wsj:fromDocument ?sentence ;
                    wsj:withmappedrole ?role .
        ?role wsj:withfnfe ?gfe ;
            rdf:value ?role_label ;
            skos:related ?location .   
        VALUES ?gfe {<https://w3id.org/framester/framenet/abox/gfe/Place>}
    }
    """

QUERY_TIMELINE = PREFIXES + \
    """
        SELECT * WHERE {
            ?event sem:hasBeginTimeStamp ?start ;
                sem:hasEndTimeStamp ?end .
        }
    """

QUERY_FRAMESTER_ROLE = """
    PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
    SELECT * WHERE {
        ?s rdf:type <https://w3id.org/framester/framenet/tbox/GenericFE> .
        OPTIONAL {?s <https://w3id.org/framester/schema/subsumedUnder> ?subsumed1}
        OPTIONAL {?subsumed1 <https://w3id.org/framester/schema/subsumedUnder> ?subsumed2}
        OPTIONAL {?subsumed2 <https://w3id.org/framester/schema/subsumedUnder> ?subsumed3}
        FILTER(CONTAINS(STR(?subsumed1), "framesterrole"))
        FILTER(CONTAINS(STR(?subsumed2), "framesterrole"))
        FILTER(CONTAINS(STR(?subsumed3), "framesterrole"))
    }
    """

QUERY_DATA_ROLE = PREFIXES + \
    """
    SELECT DISTINCT ?role_fe (COUNT(?role) as ?nb_role) WHERE {
        ?role wsj:withfnfe ?role_fe .
    }
    GROUP BY ?role_fe
    ORDER BY DESC(?nb_role)
"""

QUERY_PERSON_SEM = PREFIXES + \
    """
    SELECT ?event ?actor ?sentence_label ?gfe ?role_label ?start ?end WHERE {
        ?event sem:hasActor ?actor ;
               sem:hasBeginTimeStamp ?start ;
               sem:hasEndTimeStamp ?end .
        VALUES ?actor {<http://dbpedia.org/resource/Louis_XVI>}
    }
    """

QUERY_PERSON_SEM_FRAME = PREFIXES + \
    f"""
    SELECT ?event ?actor ?sentence_label ?gfe ?role_label ?start ?end WHERE {START}
        {START}
        ?event sem:hasActor ?actor ;
               sem:hasBeginTimeStamp ?start ;
               sem:hasEndTimeStamp ?end .
        {END}
        UNION
        {START}
        ?event ex:abstract ?abstract ;
               sem:hasBeginTimeStamp ?start ;
               sem:hasEndTimeStamp ?end .
        ?abstract rdf:value ?abstract_label ;
                    nif:sentence ?sentence .
        ?sentence rdf:value ?sentence_label .
        ?annotation rdf:type wsj:CorpusEntry ;
                    wsj:fromDocument ?sentence ;
                    wsj:withmappedrole ?role .
        ?role wsj:withfnfe ?gfe ;
                rdf:value ?role_label ;
                skos:related ?actor . 
        {END}
        
        VALUES ?gfe {START}gfe:Agent gfe:Leader gfe:Assailant gfe:Individuals gfe:Victim gfe:Person gfe:Speaker gfe:Patient gfe:Members gfe:Creator gfe:Invader gfe:Protagonist gfe:Side_1 gfe:Side_2 gfe:Experiencer gfe:New_member gfe:Owner gfe:Suspect gfe:Employee gfe:Earner{END}
        VALUES ?actor {START}<http://dbpedia.org/resource/Louis_XVI>{END}
    {END}
    """

def get_label(x):
    return unquote(x.split("/")[-1].replace("_", " "))

def get_long_lat(df):
    longitudes, latitudes, cached_long, cached_lat = [], [], {}, {}
    for _, row in tqdm(df.iterrows(), total=len(df)):
        if row.location_clean not in cached_long:
            try:
                location=GEOLOCATOR.geocode(row.location_clean)
                cached_long[row.location_clean] = location.longitude
                cached_lat[row.location_clean] = location.latitude
            except Exception as _:
                cached_long[row.location_clean] = None
                cached_lat[row.location_clean] = None
        longitudes.append(cached_long[row.location_clean])
        latitudes.append(cached_lat[row.location_clean])
    df["longitude"] = longitudes
    df["latitude"] = latitudes
    return df                


def cache_location(query, save_file):
    """ Caching location-related information """
    results = run_query(query=query, sparql_endpoint=SPARQL_ENDPOINT, headers=HEADERS_CSV)
    results["event_clean"] = results["event"].apply(get_label)
    results["location_clean"] = results["location"].apply(get_label)
    results = get_long_lat(df=results)
    results.to_csv(save_file)

def cache_time(query, save_file):
    results = run_query(query=query, sparql_endpoint=SPARQL_ENDPOINT, headers=HEADERS_CSV)
    results["event_clean"] = results["event"].apply(get_label)
    results.to_csv(save_file)

def cache_simple(query, save_file, endpoint):
    results = run_query(query=query, sparql_endpoint=endpoint, headers=HEADERS_CSV)

    if "event" in results.columns:
        results["event_clean"] = results["event"].apply(get_label)
    results.to_csv(save_file)


if __name__ == '__main__':
    FOLDER = "experiments_run/usage_ng/data"

    SAVE_FILE = os.path.join(FOLDER, "location_sem.csv")
    if not os.path.exists(SAVE_FILE):
        cache_location(query=QUERY_LOCATION_SEM,
                       save_file=SAVE_FILE)

    SAVE_FILE = os.path.join(FOLDER, "location_sem_frame.csv")
    if not os.path.exists(SAVE_FILE):
        cache_location(query=QUERY_LOCATION_SEM_FRAME,
                       save_file=SAVE_FILE)
    
    SAVE_FILE = os.path.join(FOLDER, "when_start_end.csv")
    if not os.path.exists(SAVE_FILE):
        cache_time(query=QUERY_TIMELINE,
                   save_file=SAVE_FILE)
    
    
    SAVE_FILE = os.path.join(FOLDER, "framester_roles.csv")
    if not os.path.exists(SAVE_FILE):
        cache_simple(query=QUERY_FRAMESTER_ROLE,
                     endpoint=SPARQL_ENDPOINT_FS,
                     save_file=SAVE_FILE)
    
    SAVE_FILE = os.path.join(FOLDER, "data_roles.csv")
    if not os.path.exists(SAVE_FILE):
        cache_simple(query=QUERY_DATA_ROLE,
                     endpoint=SPARQL_ENDPOINT,
                     save_file=SAVE_FILE)
    
    SAVE_FILE = os.path.join(FOLDER, "louis_xvi_sem.csv")
    if not os.path.exists(SAVE_FILE):
        cache_simple(query=QUERY_PERSON_SEM,
                     endpoint=SPARQL_ENDPOINT,
                     save_file=SAVE_FILE)
    
    SAVE_FILE = os.path.join(FOLDER, "louis_xvi_sem_frame.csv")
    if not os.path.exists(SAVE_FILE):
        cache_simple(query=QUERY_PERSON_SEM_FRAME,
                     endpoint=SPARQL_ENDPOINT,
                     save_file=SAVE_FILE)
