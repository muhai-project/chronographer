# -*- coding: utf-8 -*-
"""
Finer-grained NP encoding
"""
import spacy
import concepcy
import pandas as pd
from urllib.parse import quote
from rdflib import URIRef, Graph, Literal
from kglab.helpers.variables import NS_EX, NS_RDF, NS_SKOS
from src.build_ng.concept_net import ConceptNet

def filter_output_cn(row, info):
    """
    - df: output of calling ConceptNet class
    - info: labels (lowered, text) to POS """
    row["entity"] = row["subject"].replace("/c/en/", "").split("/")[0]
    entity = row["entity"]

    if info.get(entity) == "NOUN":
        row["filter_ent"] = 1 if row["subject"] == f"/c/en/{entity}/n" else 0
    elif info.get(entity) == "VERB":
        row["filter_ent"] = 1 if row["subject"] == f"/c/en/{entity}/a/wn" else 0
    else:
        row["filter_ent"] = 0
    return row

class NounPhraseKGEncoding:
    """ Encoding a NP into a KG """
    def __init__(self, concept_net: ConceptNet):
        nlp = spacy.load("en_core_web_sm")
        # nlp.add_pipe('concepcy')
        nlp.add_pipe(
            "dbpedia_spotlight",
            config={'confidence': 0.95, 'dbpedia_rest_endpoint': 'http://localhost:2222/rest'})
        self.nlp = nlp

        self.np_head_filter = ['NOUN', 'PROPN']
        self.np_content_filter = ['NOUN', 'PROPN', 'ADJ', 'ADP', 'NUM', 'VERB']

        self.concept_net = concept_net
        self.cn_filter = ['NOUN', 'VERB']

    def get_cn(self, doc):
        """ ConceptNet related (using a different method than spacy that is limited by the API) """
        entities = {t.text.lower(): t.pos_ for t in doc if t.pos_ in self.cn_filter}
        labels = list(entities.keys())
        output_cn = self.concept_net(labels=labels, lang="en", entity=True, relation=False)
        output_cn = output_cn.apply(lambda row: filter_output_cn(row, entities), axis=1)
        
        columns = ["subject", "predicate", "object", "entity"]
        if output_cn.shape[0] > 0:
            return output_cn[output_cn["filter_ent"] == 1][columns], entities
        return pd.DataFrame(columns=columns), entities

    def __call__(self, text: str, fe_iri: URIRef):
        graph = Graph()
        prefix = "/".join(str(fe_iri).split("/")[:-1]) + "/"
        fe_id = str(fe_iri).rsplit("/", maxsplit=1)[-1]
        doc = self.nlp(text)
        nps = [x for x in list(doc.noun_chunks) if x.root.pos_ in self.np_head_filter]
        # cn_entities = doc._.relatedto
        cn_info, cn_mapping = self.get_cn(doc=doc)
        cn_entities = list(cn_info.entity.unique())

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
                graph.add((ent_iri, NS_RDF["value"], Literal(ent)))
                if db_entities[ent][1]:
                    graph.add((ent_iri, NS_SKOS["related"], URIRef(quote(db_entities[ent][1], safe=":/"))))

            for index_t, token in enumerate([x for x in np if x.pos_ in self.np_content_filter \
                and x.text not in db_token_set]):
                token_iri = URIRef(str(np_iri) + "_" + str(index_t))
                if token == np.root:
                    predicate = NS_EX["root"]
                else:
                    predicate = NS_EX[token.pos_]
                graph.add((np_iri, predicate, token_iri))
                graph.add((token_iri, NS_RDF["value"], Literal(token.text)))

                if token.text.lower() in cn_entities:
                    cn_iri = URIRef(f"/c/en/{token.text.lower()}") \
                        if cn_mapping[token.text.lower()] == "NOUN" \
                            else URIRef(f"/c/en/{token.text.lower()}/a/wn")
                    graph.add((token_iri, NS_RDF["type"], NS_EX["CN_ENT"]))
                    graph.add((token_iri, NS_SKOS["related"], cn_iri))
                    # for x in cn_entities[token.text]:
                    #     graph.add((URIRef(x['start']['id']),
                    #                NS_EX[f"cn_rel_{x['relation']}"], URIRef(x['end']['id'])))
                    for _, row in cn_info[cn_info.entity == token.text.lower()].iterrows():
                        graph.add((URIRef(row.subject), URIRef(row.predicate), URIRef(row.object)))

        return graph


if __name__ == '__main__':
    TEXTS = [
        "the Spanish ship",
        "The French Atlantic Fleet, under Admiral Villaret de Joyeuse"
    ]
    CN_CSV = "../question_answering_ng/data/concept_net/filtered_assertions.csv"
    CONCEPT_NET = ConceptNet(api=None, cn_csv=CN_CSV)
    NP_ENCODING = NounPhraseKGEncoding(concept_net=CONCEPT_NET)
    FE_IRI = NS_EX["test"]
    for INDEX, TEXT in enumerate(TEXTS):
        GRAPH = NP_ENCODING(text=TEXT, fe_iri=FE_IRI)
        for triple in GRAPH:
            print(triple)
        print("===================")

        GRAPH.serialize(f"np_test_{INDEX}.ttl", format="ttl")
