# -*- coding: utf-8 -*-
"""

"""
import os
import click
import random
from urllib.parse import unquote
from kglab.helpers.data_load import read_csv

@click.command()
@click.option("--csv", help=".csv path containing only causation frames")
@click.option("--folder", help="save folder")
def main(csv: str, folder: str):
    df_ = read_csv(csv)
    df_["frame"] = "Causation"

    for col in ["event", "annot", "frame", "fe", "ent"]:
        df_[col] = df_[col].apply(
            lambda x: unquote(x.split("/")[-1]) if isinstance(x, str) else x)
    
    # Randomly sampling 100 annotations + seed for reproducibility
    random.seed(23)
    print(f"{df_.annot.unique().shape[0]} frames with causation")
    sampled_values = random.sample(list(df_.annot.unique()), 100)
    subset = df_[df_.annot.isin(sampled_values)]
    subset.to_csv(os.path.join(folder, "subset.csv"))

    cols_to_keep = ["event", "annot", "sent_val", "lemma", "fe", "value"]
    to_save = subset[cols_to_keep].drop_duplicates()
    to_save = to_save[to_save.fe.isin(['Cause', 'Effect'])]
    to_save.to_csv(os.path.join(folder, "causation_fe_annot.csv"))

    to_save = subset.drop_duplicates()
    to_save[to_save.fe.isin(['Cause', 'Effect'])].to_csv(
        os.path.join(folder, "causation_ent_annot.csv"))


if __name__ == '__main__':
    main()
