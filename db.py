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

    def highest_ranked(self, wiki, instance_of):
        f"""
        SELECT 
          ca.title,
          ca.pagerank,
          ef.title AS english_title
        FROM concept_instance_of instance
        INNER JOIN concept_articles cf ON (
            cf.concept_id=instance.concept_id
            AND cf.wiki='{wiki}'
        )
        INNER JOIN articles ca ON (
           ca.wiki='{wiki}'
           AND ca.article_title=cf.article_title
        ) 
        LEFT OUTER JOIN concept_articles ef ON (
            ef.concept_id=cf.concept_id
            AND ef.wiki='enwiki'
        )
        
        WHERE instance.instance_of_concept_id='{instance_of}'
        ORDER BY ca.pagerank DESC
        """

    def highest_ranked_kl(self, from_wiki, to_wiki, instance_of):
        f"""
        SELECT 
          fa.pagerank AS from_pagerank,
          ta.pagerank AS to_pagerank,
          
          ta.pagerank * (LOG(to_pagerank) - LOG(from_pagerank)) AS kl_divergence,
          
        FROM concept_instance_of instance
        INNER JOIN concept_articles cf ON (
            cf.concept_id=instance.concept_id
            AND cf.wiki='{from_wiki}'
        )
        INNER JOIN concept_articles ct ON (
            ct.concept_id=instance.concept_id
            AND ct.wiki='{to_wiki}''
        )
        INNER JOIN articles fa ON (
            fa.wiki='{from_wiki}'
            AND fa.article_title=cf.article_title
        )
        INNER JOIN articles ta ON (
            ta.wiki='{to_wiki}'
            AND ta.title=ct.article_title
        )
        WHERE instance.instance_of_concept_id='{instance_of}'
        ORDER BY kl_divergence DESC
        """

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
