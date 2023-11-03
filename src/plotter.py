# -*- coding: utf-8 -*-
""" Plotting simple line charts from results """
import pandas as pd
from pandas.core.frame import DataFrame
import plotly.express as px
from plotly.graph_objs._figure import Figure

class Plotter:
    """ Transforming metrics output into dataset and plotting figures"""
    def __init__(self):
        self.metrics = ['precision', 'recall', 'f1']

    def build_df_from_output(self, info: dict) -> DataFrame:
        """ dict -> dataframe (more suited for plotly plotting) """
        dataframe = pd.DataFrame(dict(iteration=[], value=[], type_=[]))

        for i, metrics in info.items():
            curr_df = pd.DataFrame.from_dict(
                dict(iteration=[i]*3,
                     value=[metrics[t] for t in self.metrics],
                     type_=self.metrics))
            dataframe = pd.concat([dataframe, curr_df], ignore_index = True)
        return dataframe

    @staticmethod
    def build_figure(info: dict) -> Figure:
        """ dataframe -> figure """
        fig = px.line(info, x="iteration", y="value", color='type_')
        return fig

    @staticmethod
    def save_fig(fig: Figure, path: str):
        """ fig -> dynamic html """
        fig.write_html(path)

    def __call__(self, info: dict, save_folder: str):
        dataframe = self.build_df_from_output(info)
        self.save_fig(fig=self.build_figure(dataframe),
                      path=f"{save_folder}/metrics.html")


if __name__ == '__main__':
    import os
    import json
    from settings import FOLDER_PATH
    INFO=json.load(open(
        os.path.join(FOLDER_PATH,
                     "sample-data/French_Revolution_metrics.json"),
        "r", encoding="utf-8"))
    SAVE_FOLDER = os.path.join(FOLDER_PATH, "sample-data")
    plotter=Plotter()
    plotter(info=INFO, save_folder=SAVE_FOLDER)
    print(f"Folder saved in {SAVE_FOLDER} under name `metrics.html`")
