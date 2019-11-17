import sqlite3
from contextlib import contextmanager


class WikilanguageDB:
    @classmethod
    @contextmanager
    def con(cls, path="wikilanguage.sqlite"):
        con = sqlite3.connect(path)
        try:
            yield cls(con)
        finally:
            con.close()

    def __init__(self, con):
        self.con = con

    def create_tables(self):
        with self.con as cur:
            #
            # Wikidata entries
            #

            cur.execute(
                """
                CREATE TABLE concepts (
                    concept_id TEXT PRIMARY KEY,
                    sample_title TEXT,
                    coord_latitude REAL,
                    coord_longitude REAL,
                    coord_altitude REAL,
                    coord_precision REAL
                )"""
            )

            #
            # Concept -> wikis containing articles of this concept
            #

            cur.execute(
                """
                CREATE TABLE concept_articles (
                    concept_id TEXT, 
                    wiki TEXT,
                    article_title TEXT
                )"""
            )
            cur.execute(
                """
                CREATE INDEX idx_concept_articles_concept ON concept_articles(concept_id)
                """
            )
            cur.execute(
                """
                CREATE UNIQUE INDEX idx_concept_articles_wiki_title ON concept_articles(wiki, article_title, concept_id)
                """
            )

            #
            # Flattened view of all concepts a concept is an instance of
            # (e.g. american city is an instance of city)
            #
            cur.execute(
                """
                CREATE TABLE concept_instance_of (
                    concept_id TEXT NOT NULL,
                    instance_of_concept_id TEXT NOT NULL
                )
                """
            )
            cur.execute(
                """
                CREATE UNIQUE INDEX idx_concept_id ON concept_instance_of(concept_id, instance_of_concept_id)
                """
            )
            cur.execute(
                """
                CREATE INDEX idx_concept_instance_of_id ON concept_instance_of(instance_of_concept_id)
                """
            )

            #
            # Wikipedia articles (language specific)
            #

            cur.execute(
                """
                CREATE TABLE articles (
                    wiki TEXT NOT NULL,
                    title TEXT NOT NULL,
                    id TEXT NOT NULL,
                    pagerank REAL,
                    pagerank_percentile REAL
                )
                """
            )

            cur.execute(
                """
                CREATE UNIQUE INDEX idx_articles_concept_id_wiki ON articles(wiki, title)
                """
            )
