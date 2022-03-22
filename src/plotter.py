""" Plotting simple line charts from results """
import pandas as pd
import plotly.express as px

class Plotter:
    """ Transforming metrics output into dataset and plotting figures"""
    def __init__(self):
        self.metrics = ['precision', 'recall', 'f1']

    def build_df_from_output(self, info):
        """ dict -> dataframe (more suited for plotly plotting) """
        dataframe = pd.DataFrame(dict(iteration=[], value=[], type_=[]))

        for i, metrics in info.items():
            curr_df = pd.DataFrame.from_dict(
                dict(iteration=[i]*3,
                     value=[metrics[t] for t in self.metrics],
                     type_=self.metrics))
            dataframe = dataframe.append(curr_df, ignore_index = True)
        return dataframe

    @staticmethod
    def build_figure(info):
        """ dataframe -> figure """
        fig = px.line(info, x="iteration", y="value", color='type_')
        return fig

    @staticmethod
    def save_fig(fig, path):
        """ fig -> dynamic html """
        fig.write_html(path)

    def __call__(self, info, save_folder):
        dataframe = self.build_df_from_output(info)
        self.save_fig(fig=self.build_figure(dataframe),
                      path=f"{save_folder}/metrics.html")


if __name__ == '__main__':
    SAVE_FOLDER="/Users/ines/Projects/graph_search_framework"
    import os
    import json
    from settings import FOLDER_PATH
    INFO=json.load(open(
        os.path.join(FOLDER_PATH,
                     "data/2022-03-01-14:56:59-iter-30-triply-entropy_pred_object_freq/metrics.json"),
        "r", encoding="utf-8"))
    plotter=Plotter()
    plotter(INFO, SAVE_FOLDER)
