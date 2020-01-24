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

def haversine(df, src_lat, src_lng):
    target_lat = df["coord_latitude"]
    target_lng = df["coord_longitude"]
    src_lng, src_lat, target_lng, target_lat = map(np.radians, (src_lng, src_lat, target_lng, target_lat))
    dlon = target_lng - src_lng 
    dlat = target_lat - src_lat 
    a = sin(dlat/2)**2 + cos(src_lat) * cos(src_lat) * sin(dlon/2)**2
    c = 2 * arcsin(sqrt(a)) 
    km = 6367 * c
    return km

# Boolean indexers
def instance_of(df, instance_of_id):
    return df["instance_of"].str.contains(f"{instance_of_id}[,$]", na=False)


def country_of_origin(df, concept_id):
    return df["country_of_origin"] == concept_id

def within_radius(df, lat, lng, radius_km=50):
    return haversine(df, lat, lng) < radius_km
    
def publication_year(df, year):
    return df["publication_date"].dt.to_period("A") == str(year)


def resolve_label(df, label_name):
    vals = df.loc[df["enwiki_title"] == label_name]
    assert len(vals) == 1, f"Too many or few rows found ({len(vals)})"
    return vals.index.values[0]


def best_concepts(df, sample=0.1, n=200):
    instances_of = df["instance_of"].sample(frac=sample).str.split(",").explode()
    prob_mass = (
        pd.concat(
            (instances_of, df.loc[instances_of.index]["enwiki_pagerank"]),
            axis=1,
            copy=False,
        )
        .groupby(["instance_of"])
        .sum()
        .nlargest(n, "enwiki_pagerank")
    )
    ret = pd.concat(
        (df.loc[prob_mass.index]["sample_label"], prob_mass), axis=1, copy=False
    ).dropna()
    return ret


def top_ranked(df, wiki, n=200, desc=True):
    ret = df[df[f"{wiki}_title"].notna()][
        ["sample_label", f"{wiki}_pagerank"]
    ]
    ret[f"{wiki}_relative_to_max"] = (
        ret[f"{wiki}_pagerank"] / ret[f"{wiki}_pagerank"].max()
    )
    if desc:
        return ret.nlargest(n, f"{wiki}_pagerank")
    else:
        return ret.nsmallest(n, f"{wiki}_pagerank")
    


def top_by_year(df, top_col="enwiki_pagerank", date_col="publication_date", n=200):
    by_year = df.groupby(df[date_col].dt.to_period("A"))[top_col].idxmax().dropna()
    return df.loc[by_year].nlargest(n, date_col)


def kl_divergence(
    df, base_wiki, target_wiki, n=200, marginals=True, importance_weight=1
):
    matching_rows = df.loc[
        df[f"{base_wiki}_title"].notna() & df[f"{target_wiki}_title"].notna()
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
