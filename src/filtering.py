"""
Filtering class: Filtering out certain types of nodes
- Given a specific narrative dimension (who/where/when)
- Two types of filtering can be applied
    - Filtering on subgraph: node that matches the node type to be searched,
    but some properties don't match
        --> filtered out from subgraph, and removed from expandable nodes space
    - Filtering on pending nodes (nodes that could be expanded): same than above but for expansion
        --> removed from expandable nodes space
"""
import re
import pandas as pd

class Filtering:
    """
    Main Filtering class for subgraph and pending nodes

    """
    def __init__(self, args):
        """
        - args dict: keys = narrative dimensions, value = boolean
        """
        self._check_args(args=args)
        self.where = args["where"] if "where" in args else 0
        self.when = args["when"] if "when" in args else 0

        self.dates = [
            "http://dbpedia.org/ontology/date"
        ]
        self.start_dates = [
            "http://dbpedia.org/ontology/startDate",
            "http://dbpedia.org/property/birthDate"
        ]
        self.end_dates = [
            "http://dbpedia.org/ontology/endDate",
            "http://dbpedia.org/property/deathDate"
        ]

        self.temporal = self.dates + self.start_dates + self.end_dates

        self.places = [
            "http://dbpedia.org/ontology/Place",
            "http://dbpedia.org/ontology/Location"
        ]

    @staticmethod
    def _check_args(args):
        for val in ["where", "when"]:
            if val in args and args[val] not in [0, 1]:
                raise ValueError(f"`{val}` value from args should be 0 or 1 (int)")

    def get_to_discard_date(self, date_df: pd.core.frame.DataFrame, dates: list[str]):
        """ Filtering on temporal dimension
        - checking date/start date/end date """

        return list(date_df[((date_df.predicate.isin(self.end_dates)) & \
                             (date_df.object < dates[0])) | \
                            ((date_df.predicate.isin(self.start_dates)) & \
                             (date_df.object > dates[1])) | \
                            ((date_df.predicate.isin(self.dates)) & \
                             (date_df.object < dates[0])) | \
                            ((date_df.predicate.isin(self.dates)) & \
                             (date_df.object > dates[1]))].subject.unique())

    @staticmethod
    def regex_helper(row, default_return_val):
        """ Finding regex column name in subject str uri """
        pattern = "\\d{4}"
        matches = re.findall(pattern, row.subject)
        if matches:
            return str(matches[0])
        return default_return_val


    def get_to_discard_regex(self, df_pd: pd.core.frame.DataFrame, dates: list[str]):
        """ Filtering on string uri
        - temporal dimension: regex on the URL (and therefore name of the events,
            e.g. 1997_National_Championships > non relevant """
        df_pd['regex_helper']= df_pd.apply(
            lambda x: self.regex_helper(x, dates[0]), axis=1)

        return list(df_pd[(df_pd.regex_helper < dates[0][:4]) | \
                          (df_pd.regex_helper > dates[1][:4])].subject.unique())

    def get_to_discard_location(self, df_pd: pd.core.frame.DataFrame):
        """ Location filter: retrieving nodes that correspond to locations
        (would be too broad for the search, hence later discarded """
        return list(df_pd[df_pd.object.isin(self.places)].subject.unique())

    def __call__(self, df_pd: pd.core.frame.DataFrame, dates: list[str]):
        """
        """
        date_df = df_pd[df_pd.predicate.isin(self.temporal)]
        date_df.object = date_df.object.astype(str)

        to_discard = []

        if self.where:
            to_discard += list(set(self.get_to_discard_location(df_pd=df_pd)))

        if self.when:
            to_discard += list(set(self.get_to_discard_date(date_df=date_df, dates=dates) + \
                              self.get_to_discard_regex(df_pd=df_pd, dates=dates)))

        return to_discard
