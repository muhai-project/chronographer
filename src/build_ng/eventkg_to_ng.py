# -*- coding: utf-8 -*-
"""
Converting EventKG to a format that is comparable to the Narrative Graphs (NGs) built from the graph search
"""
import click
from tqdm import tqdm
from requests.models import Response
from rdflib import Graph
from kglab.helpers.encoding import encode
from kglab.helpers.kg_query import run_query
from kglab.helpers.kg_build import init_graph
from kglab.helpers.data_load import read_csv
from kglab.helpers.variables import EVENTKG_ENDPOINT, PREFIX_SEM, STR_SEM, PREFIX_RDF, STR_RDF, \
    PREFIX_OWL, STR_OWL, NS_SEM, NS_RDF, NS_OWL, HEADERS_RDF_XML

class EventKGToNGConverter:
    """ Converting sub-graphs of EventKG to simplified SEM """
    def __init__(self, endpoint: str = EVENTKG_ENDPOINT):
        """ Init: endpoint """
        self.endpoint = endpoint
        self.construct_template = self._init_construct_template()

        self.prefix_to_ns = {
            PREFIX_SEM: NS_SEM, PREFIX_RDF: NS_RDF,
            PREFIX_OWL: NS_OWL
        }

    def _init_construct_template(self):
        """
        - <event-input>: event to construct the NG from
        - <filter-str>: filtering KB provenance (eg "/dbpedia")
        """
        start, end = "{", "}"
        prefixes = f"""
        PREFIX {PREFIX_SEM}: <{STR_SEM}>
        PREFIX {PREFIX_OWL}: <{STR_OWL}>
        PREFIX {PREFIX_RDF}: <{STR_RDF}>
        """

        template_place = f"""
            {prefixes}
            CONSTRUCT {start}
                <event-input> sem:hasPlace ?kb_place  .
            {end}
            WHERE
            {start}
                ?s owl:sameAs <event-input> .

                GRAPH eventkg-g:<filter-named-graph> {start}
                    ?s sem:hasPlace ?place .
                    ?place owl:sameAs ?kb_place .
                {end}

            {end}
            """

        template_actor = f"""
            {prefixes}
            CONSTRUCT {start}
                <event-input> sem:hasActor ?kb_actor .
            {end}
            WHERE
            {start}
                ?s owl:sameAs <event-input> .
                ?subject_rel rdf:type sem:Actor ;
                            owl:sameAs ?kb_actor .

                GRAPH eventkg-g:<filter-named-graph> {start}
                ?rel rdf:type eventkg-s:Relation ;
                        rdf:subject ?s ;
                        rdf:object ?subject_rel ;
                        sem:roleType ?role .
                {end}

                FILTER(CONTAINS(str(?kb_actor), "<filter-str>"))
            {end}
            """

        template_bts = f"""
            {prefixes}
            CONSTRUCT {start}
                <event-input> sem:hasBeginTimeStamp ?b_timestamp .
            {end}
            WHERE
            {start}
                ?s owl:sameAs <event-input> .

                GRAPH eventkg-g:<filter-named-graph>   
                {start}
                    OPTIONAL {start}?s sem:hasBeginTimeStamp ?b_timestamp .{end}
                {end}
            {end}
            """

        template_ets = f"""
            {prefixes}
            CONSTRUCT {start}
                <event-input> sem:hasEndTimeStamp ?e_timestamp .
            {end}
            WHERE
            {start}
                ?s owl:sameAs <event-input> .

                GRAPH eventkg-g:<filter-named-graph>   
                {start}
                    OPTIONAL {start}?s sem:hasEndTimeStamp ?e_timestamp .{end}
                {end}
            {end}
            """

        template_event = f"""
            {prefixes}
            CONSTRUCT {start}
                <event-input> rdf:type sem:Event .
            {end}
            WHERE {start}
                ?s owl:sameAs <event-input> .
            {end}
            """
        
        template_sub_event = f"""
            {prefixes}
            CONSTRUCT {start}
                ?kb_sub_event sem:subEventOf <event-input> .
            {end}
            WHERE {start}
                ?s owl:sameAs <event-input> .
                ?s sem:subEventOf ?super_event .
                ?super_event owl:sameAs ?kb_super_event .
                FILTER(CONTAINS(str(?kb_super_event), "<filter-str>"))
            {end}
            """
        
        template_super_event = f"""
            {prefixes}
            CONSTRUCT {start}
                <event-input> sem:subEventOf ?kb_super_event .
            {end}
            WHERE {start}
                ?s owl:sameAs <event-input> .
                ?s sem:hasSubEvent ?sub_event .
                ?sub_event owl:sameAs ?kb_sub_event .
                FILTER(CONTAINS(str(?kb_sub_event), "<filter-str>"))
            {end}
            """

        return [template_place, template_actor, template_bts, template_ets, template_event,
                template_sub_event, template_super_event]

    def construct_one_sub_ng(self, template: str, event: str, filter_str: str, filter_named_graph: str) -> Response:
        """ From one event + one KB, constructs the NG """
        query = template.replace("event-input", encode(text=event)) \
            .replace("<filter-str>", filter_str) \
                .replace("<filter-named-graph>", filter_named_graph)
        return run_query(query=query, sparql_endpoint=self.endpoint, headers=HEADERS_RDF_XML)

    def __call__(self, events: list[str], filter_str: str, filter_named_graph: str) -> Graph:
        """ events: list of events """
        graph = init_graph(prefix_to_ns=self.prefix_to_ns)

        for event in tqdm(events):
            for template in self.construct_template:
                response = self.construct_one_sub_ng(template=template, event=event, filter_str=filter_str,
                                                     filter_named_graph=filter_named_graph)
                if response.status_code == 200:
                    graph.parse(data=response.text, format="application/rdf+xml")
                else:
                    print(f"Request failed for event {event}")
                    print(f"Text response: {response.text}")
        return graph
        

@click.command()
@click.option("--csv", help=".csv path to list of events")
@click.option("--fs", help="filter string based on KG, eg. `/dbpedia` or `wikidata`")
@click.option("--fng", help="filter named graph for query, eg. `dbpedia_en` or `wikidata`")
@click.option("--save", help=".ttl save path for KG")
def main(csv: str, fs: str, fng: str, save: str):
    df = read_csv(path=csv)
    events = df.linkDBpediaEn.values[:10]
    print(events)

    converter = EventKGToNGConverter()
    graph = converter(events=events, filter_str=fs, filter_named_graph=fng)
    graph.serialize(save, format="ttl")
        

if __name__ == '__main__':
    """ 
    Build one graph from EventKG.
    Example below from root directory:
    python src/build_ng/eventkg_to_ng.py --csv ./sample-data/French_Revolution_gs_events.csv \
        --fs /dbpedia --fng dbpedia_en --save eventkg_ng.ttl 
    """
    main()
