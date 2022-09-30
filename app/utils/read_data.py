""" Mainly opening files + some pre-processing """
import pickle

def read_pickled_data_graph_search(folder):
    """ Modify index of certain df for better display """
    data = pickle.load(open(f"{folder}/framework.pkl", 'rb'))
    for key, col in [("path_expanded", "iteration"),
                     ("nodes_expanded_per_iter", "iteration")]:
        data[key].set_index(col, inplace=True)
    return data
