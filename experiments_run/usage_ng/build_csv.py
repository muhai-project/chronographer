# -*- codinf: utf-8 -*-
"""
From Question-Answering, build a csv with the following columns:
- prompt_base
- answer_base
- prompt_triple
- answer_triple
"""
import os
import click
import pandas as pd


@click.command()
@click.argument("folder")
def main(folder):
    """ Running all that can be found in the folder.
    The `folder` should contain the following two sub-folders:
    - gpt_base: all content related to base
    - gpt_triples: all content related to triples
    -------------------------------------------------------------
    Each of the above two sub-folders contain the followings:
    - `prompts`: contain the prompts to be run on GPT
    - `answers`: the answers will be saved in this folder """
    names = os.listdir(os.path.join(folder, "gpt_base", "answers"))
    df = pd.DataFrame(columns=["id", "prompt_base", "answer_base",
                               "prompt_triple", "answer_triple"])
    for name in names:
        curr_res = [name.replace(".txt", "")]
        for sub_f in ["gpt_base/prompts", "gpt_base/answers",
                      "gpt_triples/prompts", "gpt_triples/answers"]:
            with open(os.path.join(folder, sub_f, name), "r", encoding="utf-8") as file:
                content = file.read()
                curr_res.append(f'"{content}"')
        df.loc[len(df)] = curr_res
    df.sort_values(by="id").to_csv("prompts_answers.csv")


if __name__ == '__main__':
    main()
