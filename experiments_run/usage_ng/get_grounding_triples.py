# -*- codinf: utf-8 -*-
"""
For grounding metric
"""
import json
import click
from urllib.parse import unquote
from dbpedia_save_prompts import get_triples_prompt_df as get_prompt_db
from save_prompts import get_triples_prompt_df as get_prompt_ng

ID_NODES = {
    "summary": {
        "event": ["French_Revolution"],
        "sub_event": ["Storming_of_the_Bastille"]
    },
    "cause_consequence": {
        "event": ["French_Revolution"],
        "sub_event": ["Battle_of_Winterthur"]
    },
    "event_type_timestamped": {
        "periods": [("1789-01-01", "1790-01-01"), ("1792-01-01", "1793-01-01")]
    },
    "sub_events_of_event": {
        "event": ["War_of_the_Second_Coalition", "French_Revolutionary_Wars"]
    },
    "actor_event": {
        "actor": ["Antoine_Balland",
                  "Jacques_Gilles_Henri_Goguet"]
    },
    "actor_common": {
        "actor": [("Jean-Baptiste_Jourdan", "Joseph_Bonaparte"),
                  ("Charles_IV_of_Spain", "Francis_II%2C_Holy_Roman_Emperor"),
                  ("Guillaume_Brune", "Magnus_Gustav_von_Essen")]
    },
}

@click.command()
@click.argument("save_path")
def main(save_path):
    """ Prompts for grounding """
    res = {}
    for type_id, info in ID_NODES.items():
        for type_info, vals in info.items():
            for val in vals:
                triples_db = get_prompt_db(type_id=type_id, val=val)
                events = list(triples_db.subject.unique())
                triples_ng = get_prompt_ng(type_id=type_id, val=val)
                events += [x for x in list(triples_ng.subject.unique()) if not x[-1].isdigit()]
                events = list(set(unquote(x) for x in events))
                res[f"{type_id}_{type_info}_{val}"] = events
    with open(save_path, "w", encoding="utf-8") as openfile:
        for k, v in res.items():
            events = '\n'.join(v)
            openfile.write(f"{k}\n{len(v)} events\n----------\n\n{events}\n\n==========\n\n")


if __name__ == '__main__':
    main()

