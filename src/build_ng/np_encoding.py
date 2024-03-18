# -*- coding: utf-8 -*-
"""
Finer-grained NP encoding
"""
import spacy
import concepcy
from rdflib import URIRef, Graph, Literal
from kglab.helpers.variables import NS_EX, NS_RDF, NS_SKOS

class NounPhraseKGEncoding:
    """ Encoding a NP into a KG """
    def __init__(self):
        nlp = spacy.load("en_core_web_sm")
        nlp.add_pipe('concepcy')
        nlp.add_pipe(
            "dbpedia_spotlight",
            config={'confidence': 0.95, 'dbpedia_rest_endpoint': 'http://localhost:2222/rest'})
        self.nlp = nlp

        self.np_head_filter = ['NOUN', 'PROPN']
        self.np_content_filter = ['NOUN', 'PROPN', 'ADJ', 'ADP', 'NUM', 'VERB']
    
    def __call__(self, text: str, fe_iri: URIRef):
        graph = Graph()
        prefix = "/".join(str(fe_iri).split("/")[:-1]) + "/"
        fe_id = str(fe_iri).split("/")[-1]
        doc = self.nlp(text)
        nps = [x for x in list(doc.noun_chunks) if x.root.pos_ in self.np_head_filter]
        cn_entities = doc._.relatedto

        for index_np, np in enumerate(nps):
            db_entities = {e.text: (e.label_, e.kb_id_) for e in np.ents}
            db_token_set = set(x for ent in db_entities for x in ent.split())
            np_iri = URIRef(prefix + fe_id + "_" + str(index_np))
            graph.add((fe_iri, NS_EX["hasNP"], np_iri))
            graph.add((np_iri, NS_RDF["value"], Literal(np.text)))

            for ent in db_entities:
                ent_iri = URIRef(str(np_iri) + "_" + "_".join(ent.split()))
                predicate = NS_EX["ent"]
                graph.add((np_iri, predicate, ent_iri))
                graph.add((ent_iri, NS_RDF["type"], NS_EX[db_entities[ent][0]]))
                if db_entities[ent][1]:
                    graph.add((ent_iri, NS_SKOS["related"], URIRef(db_entities[ent][1])))

            for index_t, token in enumerate([x for x in np if x.pos_ in self.np_content_filter \
                and x.text not in db_token_set]):
                token_iri = URIRef(str(np_iri) + "_" + str(index_t))
                if token == np.root:
                    predicate = NS_EX["root"]
                else:
                    predicate = NS_EX[token.pos_]
                graph.add((np_iri, predicate, token_iri))
                graph.add((token_iri, NS_RDF["value"], Literal(token.text)))

                if token.text in cn_entities:
                    graph.add((token_iri, NS_RDF["type"], NS_EX["CN_ENT"]))
                    graph.add((token_iri, NS_SKOS["related"], URIRef(f"/c/en/{token.lemma_}")))
                    for x in cn_entities[token.text]:
                        graph.add((URIRef(x['start']['id']), NS_EX[f"cn_rel_{x['relation']}"], URIRef(x['end']['id'])))
        
        return graph


if __name__ == '__main__':
    TEXTS = [
        "the Spanish ship",
        "The French Atlantic Fleet, under Admiral Villaret de Joyeuse"
    ]
    NP_ENCODING = NounPhraseKGEncoding()
    FE_IRI = NS_EX["test"]
    for INDEX, TEXT in enumerate(TEXTS):
        GRAPH = NP_ENCODING(text=TEXT, fe_iri=FE_IRI)
        for triple in GRAPH:
            print(triple)
        print("===================")

        GRAPH.serialize(f"np_test_{INDEX}.ttl", format="ttl")
