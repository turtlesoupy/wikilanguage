import pandas as pd
import numpy as np
import functools


class Concepts:
    FILM = "Q11424"
    CITY = "Q515"
    VIDEO_GAME = "Q7889"

    """
    "city": d("Q2095"),
    "food": d("Q515"),
    "human": d("Q5"),
    "country": d("Q6256"),
    "year": d("Q577"),
    "tourist attraction": d("Q570116"),
    "archaeological site": d("Q839954"),
    "temple": d("Q44539"),
    "job": d("Q192581"),
    "higher_education": d("Q38723"),
    "anime film": d("Q20650540"),
    "film": d("Q11424"),
    "building": d("Q41176"),
    "mountain": d("Q8502"),
    "trail": d("Q628179"),
    "event": d("Q1656682"),
    "television series": d("Q5398426"),
    "website": d("Q35127"),
    "language": d("Q34770"),
    "human-geographic": d("Q15642541"),
    "political-territorial-entity": d("Q1048835"),
    """


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


# Boolean indexers
def instance_of(df, instance_of_id):
    return df["instance_of"].str.contains(f"{instance_of_id}[,$]", na=False)


def country_of_origin(df, concept_id):
    return df["country_of_origin"] == concept_id


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


def top_ranked(df, wiki, n=200):
    ret = df[df[f"{wiki}_title"].notna()][
        ["enwiki_title", f"{wiki}_title", f"{wiki}_pagerank"]
    ]
    ret[f"{wiki}_relative_to_max"] = (
        ret[f"{wiki}_pagerank"] / ret[f"{wiki}_pagerank"].max()
    )
    return ret.nlargest(n, f"{wiki}_pagerank")


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
