# -*- codinf: utf-8 -*-
"""
SPARQL queries to add triples/n-ary to the prompting
"""

PREFIXES = """
PREFIX dbr: <http://dbpedia.org/resource/>
PREFIX ex: <http://example.com/>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
PREFIX nif: <http://persistence.uni-leipzig.org/nlp2rdf/ontologies/nif-core#>
PREFIX dbo: <http://dbpedia.org/ontology/>
PREFIX frame: <https://w3id.org/framester/framenet/abox/frame/> 
PREFIX wsj: <https://w3id.org/framester/wsj/> 
PREFIX sem: <http://semanticweb.cs.vu.nl/2009/11/sem/> 
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#> 
PREFIX gfe: <https://w3id.org/framester/framenet/abox/gfe/>
"""

QUERY_INFO_EVENT = PREFIXES + """
CONSTRUCT {
    ?node ?p ?o ;
          ex:abstract ?abstract_val .
    #?role skos:related ?node ;
    #      rdf:value ?role_val ;
    #      wsj:withfnfe ?fe .
    ?node skos:related ?event ;
          skos:related ?role .
    ?role ex:withrole ?fe .
    ?event ex:abstract ?doc_val .
} WHERE {
    {
    ?node ?p ?o .
    OPTIONAL {
            ?node ex:abstract ?abstract .
            ?abstract rdf:value ?abstract_val .
        }
        FILTER(?p != ex:abstract)
    }
    UNION
    {
    ?role skos:related ?node ;
          rdf:value ?role_val ;
          wsj:withfnfe ?fe .
    ?annot wsj:withmappedrole ?role ;
           wsj:onLemma ?lemma ;
           wsj:onFrame ?frame ;
           wsj:fromDocument ?doc .
    ?doc rdf:value ?doc_val .
    ?abstract_ nif:sentence ?doc ;
               rdf:value ?abstract__val .
    ?event ex:abstract ?abstract_ .
    }
    VALUES ?node {dbr:<event>}
}
"""

QUERY_INFO_CAUSES_CONSEQUENCES = PREFIXES + """
CONSTRUCT {
    ?event ex:abstract ?abstract .
    ?abstract ex:hasValue ?abstract_label .
    ?abstract ex:hasSentence ?sentence_label .
    ?entity skos:related ?event ;
            skos:related ?role .
    ?role ex:withrole ?fe . 
} WHERE {
    ?event ex:abstract ?abstract .
    ?abstract rdf:value ?abstract_label ;
              nif:sentence ?sentence .
    ?sentence rdf:value ?sentence_label .
    ?annotation rdf:type wsj:CorpusEntry ;
                wsj:fromDocument ?sentence ;
                wsj:onFrame ?frame ;
                wsj:withmappedrole ?role .
   	?sentence rdf:value ?sentence_label .
    ?role wsj:withfnfe ?fe ;
          rdf:value ?role_label .
    OPTIONAL {?role skos:related ?entity .}
    VALUES ?frame {frame:Causation frame:Cause_harm frame:Process_end frame:Cause_to_end}
    VALUES ?event {dbr:<event>}   
}
"""

QUERY_EVENT_TYPE_TIMESTAMPED = PREFIXES + """
CONSTRUCT {
    ?event rdf:type ?frame .
} WHERE {
    ?event ex:abstract ?abstract ;
           sem:hasBeginTimeStamp ?begin_ts ;
           sem:hasEndTimeStamp ?end_ts .
    ?abstract rdf:value ?abstract_label ;
              nif:sentence ?sentence .
    ?annotation rdf:type wsj:CorpusEntry ;
                wsj:onFrame ?frame ;
                wsj:fromDocument ?sentence .
    FILTER(((?begin_ts >= "<start_date>T00:00:00"^^xsd:dateTime) && 
            (?begin_ts <= "<end_date>T00:00:00"^^xsd:dateTime)) ||
            ((?end_ts >= "<start_date>T00:00:00"^^xsd:dateTime) &&
             (?end_ts <= "<end_date>T00:00:00"^^xsd:dateTime)))
}
"""

QUERY_SUB_EVENTS_OF_EVENT = PREFIXES + """
CONSTRUCT {
    ?sub_event sem:subEventOf ?event ;
               ex:abstract ?abstract_label ;
               sem:hasBeginTimeStamp ?begin_ts ;
               sem:hasEndTimeStamp ?end_ts .
    ?event ex:abstract ?abstract__label .
} WHERE {
    ?sub_event sem:subEventOf ?event ;
               sem:hasBeginTimeStamp ?begin_ts ;
               sem:hasEndTimeStamp ?end_ts .
    OPTIONAL {
        ?sub_event ex:abstract ?abstract .
        ?abstract rdf:value ?abstract_label .}
    OPTIONAL {
        ?event ex:abstract ?abstract_ .
        ?abstract_ rdf:value ?abstract__label .}
    VALUES ?event {dbr:<event>}
}
"""