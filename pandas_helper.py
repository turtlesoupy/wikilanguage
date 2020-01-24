import pandas as pd
import numpy as np
import functools
from numpy import cos, sin, arcsin, sqrt
from math import radians

class Concepts:
    FILM = "Q11424"
    CITY = "Q515"
    VIDEO_GAME = "Q7889"
    MUSEUM = "Q33506"
    SOCIAL_ISSUE = "Q1920219"
    CREATIVE_WORK = "Q17537576"
    POLITICAL_POWER = "Q2101636"
    HUMAN = "Q5"
    MUSICAL_GROUP = "Q215380"
    WRITTEN_WORK = "Q47461344"
    MUSICAL_WORK = "Q2188189"
    
class Countries:
    FRANCE = "Q142"
    UNITED_KINGDOM = "Q145"
    JAPAN = "Q17"
    

def load_data(datapath="data/wikilanguage.tsv", usecols=None, limit=None):
    headers = next(open(datapath)).split()
    wikis = set(e.split("_")[0] for e in headers if "wiki_" in e)
    df = pd.read_csv(
        datapath,
        sep="\t",
        index_col="concept_id",
        usecols=usecols,
        nrows=limit,
        dtype={
            "concept_id": "str",
            "instance_of": "str",
            "direct_instance_of": "str",
            "sample_label": "str",
            "country_of_origin": "str",
            "publication_date": "float",
            **{f"{wiki}_title": "unicode" for wiki in wikis},
            **{f"{wiki}_pagerank": "float64" for wiki in wikis},
        },
    )

    df["publication_date"] = pd.to_datetime(
        df["publication_date"], unit="s", errors="coerce"
    )

    for wiki in wikis:
        if usecols is None or f"{wiki}_pagerank" in usecols:
            df[f"{wiki}_pagerank"] /= df[f"{wiki}_pagerank"].sum()

    return df

    
@pd.api.extensions.register_dataframe_accessor("wl")
class WikilanguageAccessor:
    def __init__(self, pandas_obj):
        self._validate(pandas_obj)
        self._df = pandas_obj
        self.concepts = Concepts

    @staticmethod
    def _validate(obj):
        # verify there is a column latitude and a column longitude
        if 'coord_latitude' not in obj.columns or 'coord_longitude' not in obj.columns:
            raise AttributeError("Must have 'latitude' and 'longitude'.")
            
    def haversine(self, src_lat, src_lng):
        target_lat = self._df["coord_latitude"]
        target_lng = self._df["coord_longitude"]
        src_lng, src_lat, target_lng, target_lat = map(np.radians, (src_lng, src_lat, target_lng, target_lat))
        dlon = target_lng - src_lng 
        dlat = target_lat - src_lat 
        a = sin(dlat/2)**2 + cos(src_lat) * cos(src_lat) * sin(dlon/2)**2
        c = 2 * arcsin(sqrt(a)) 
        km = 6367 * c
        return km
    
    def resolve_label(self, label_name, col="sample_label"):
        vals = self._df.loc[self._df[col] == label_name]
        assert len(vals) == 1, f"Too many or few rows found ({len(vals)})"
        return vals.index.values[0]
    
    def resolve(self, label_name, col="sample_label"):
        return self._df.loc[[self.resolve_label(label_name, col)]]

    # Boolean indexers
    def is_instance_of(self, instance_of_id, direct=False):
        key = "direct_instance_of" if direct else "instance_of"
        return self._df[key].str.contains(f"{instance_of_id}[,$]", na=False)

    def instance_of(self, instance_of_id, direct=False):
        return self._df[self.is_instance_of(instance_of_id, direct=direct)]

    def is_country_of_origin(self, concept_id):
        return self._df["country_of_origin"] == concept_id
    
    def country_of_origin(self, concept_id):
        return self._df[self.is_country_of_origin(concept_id)]
    
    def is_within_radius(self, lat, lng, radius_km=50):
        return self.haversine(lat, lng) < radius_km
    
    def within_radius(self, other, radius_km=50):
        if len(self._df) == 1:
            lat, lng = self._df.iloc[0]["coord_latitude"], self._df.iloc[0]["coord_longitude"]
            return other[other.wl.is_within_radius(lat, lng, radius_km)]    
        elif len(other) == 1:
            lat, lng = other.iloc[0]["coord_latitude"], other["coord_longitude"]
            return self._df[self._df.wl.is_within_radius(lat, lng, radius_km)]
        else:
            raise RuntimeError(f"Need exactly one row for nearby")
        

    def is_publication_year(self, year):
        return self._df["publication_date"].dt.to_period("A") == str(year)
    
    def publication_year(self, year):
        return self._df[self._is_publication_year(year)]
    
    # Outputs
    def best_concepts(self, sample=0.1, n=200, direct=False):
        key = "direct_instance_of" if direct else "instance_of"
        instances_of = self._df[key].sample(frac=sample).str.split(",").explode()
        prob_mass = (
            pd.concat(
                (instances_of, self._df.loc[instances_of.index]["enwiki_pagerank"]),
                axis=1,
                copy=False,
            )
            .groupby([key])
            .sum()
            .nlargest(n, "enwiki_pagerank")
        )
        ret = pd.concat(
            (self._df.loc[prob_mass.index]["sample_label"], prob_mass), axis=1, copy=False
        ).dropna()
        return ret


    def top_ranked(self, wiki, n=200, desc=True):
        ret = self._df[self._df[f"{wiki}_title"].notna()][
            ["sample_label", f"{wiki}_pagerank"]
        ]
        ret[f"{wiki}_relative_to_max"] = (
            ret[f"{wiki}_pagerank"] / ret[f"{wiki}_pagerank"].max()
        )
        if desc:
            return ret.nlargest(n, f"{wiki}_pagerank")
        else:
            return ret.nsmallest(n, f"{wiki}_pagerank")

    def top_by_year(self, top_col="enwiki_pagerank", date_col="publication_date", n=200):
        by_year = self._df.groupby(self._df[date_col].dt.to_period("A"))[top_col].idxmax().dropna()
        return self._df.loc[by_year].nlargest(n, date_col)


    def kl_divergence(
        self, base_wiki, target_wiki, n=200, marginals=True, importance_weight=1
    ):
        matching_rows = self._df.loc[
            self._df[f"{base_wiki}_title"].notna() & self._df[f"{target_wiki}_title"].notna()
        ].copy()
        if marginals:
            matching_rows[f"{base_wiki}_pagerank"] /= matching_rows[
                f"{base_wiki}_pagerank"
            ].sum()
            matching_rows[f"{target_wiki}_pagerank"] /= matching_rows[
                f"{target_wiki}_pagerank"
            ].sum()
        matching_rows["kl_divergence"] = (
            matching_rows[f"{target_wiki}_pagerank"] ** importance_weight
        ) * (
            np.log(matching_rows[f"{target_wiki}_pagerank"])
            - np.log(matching_rows[f"{base_wiki}_pagerank"])
        )
        matching_rows["odds_ratio"] = (
            matching_rows[f"{target_wiki}_pagerank"]
            / matching_rows[f"{base_wiki}_pagerank"]
        )

        matching_rows["kl_relative_to_max"] = (
            matching_rows["kl_divergence"] / matching_rows["kl_divergence"].max()
        )
        return matching_rows
