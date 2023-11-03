# -*- coding: utf-8 -*-
"""
Selecting nodes to expand based on a node list

First implemented: all nodes that correspond to a path
Straightforward version: random choice
"""
import random

class NodeSelection:
    """
    Main class for node selection
    """
    def __init__(self, mode: str = "all"):
        self.mode = mode

        self.mode_to_getter = {
            "all": lambda nodes: (nodes, []),
            "random": self.get_random
        }

    @staticmethod
    def get_random(nodes: list[str]) -> (list[str], list[str]):
        """ Selecting random node to expand """
        node = random.choice(nodes)
        nodes.remove(node)
        return ([node], nodes)

    def __call__(self, nodes):
        return self.mode_to_getter[self.mode](nodes)
