# -*- codinf: utf-8 -*-
"""
Generating answers
"""
import os
import click
from tqdm import tqdm
from openai import OpenAI
from settings import API_KEY_GPT

CLIENT = OpenAI(api_key=API_KEY_GPT)
MODEL = "gpt-4-0125-preview"

def run_gpt(prompt):
    """ Get answer from GPT """
    completion = CLIENT.chat.completions.create(
        model=MODEL,
        messages = [
            {"role": "user", "content": prompt}
        ],
        temperature=0)
    return completion.choices[0].message.content

@click.command()
@click.argument("folder")
def main(folder):
    """ Running all that can be found in the folder. The `folder` should contain two sub-folders:
    - `prompts`: contain the prompts to be run on GPT
    - `answers`: the answers will be saved in this folder """
    prompts = set(os.listdir(os.path.join(folder, "prompts")))
    answers = set(os.listdir(os.path.join(folder, "answers")))

    to_run = set(prompts.difference(answers))
    for prompt_path in tqdm(to_run, unit="item"):
        with open(os.path.join(folder, "prompts", prompt_path), encoding="utf-8") as f:
            prompt = f.read()
        answer = run_gpt(prompt=prompt)
        with open(os.path.join(folder, "answers", prompt_path), "w+", encoding="utf-8") as file:
            file.write(answer)


if __name__ == '__main__':
    main()
