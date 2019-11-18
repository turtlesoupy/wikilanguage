import itertools
import tempfile
import pipelines
from db import WikilanguageDB
import time
import os
import glob
from collections import defaultdict, OrderedDict
from wikidata_parser import WikiDataParser, WikiDataInheritanceGraph


def main():
    wikidata_path = "wikis-2019-11-17/wikidata-latest-all.json.gz"
    wiki_paths = glob.glob("wikis-2019-11-17/*-pages-articles*")
    inheritance_path = "data/wikidata_inheritance.pickle"
    db_path = "data/wikilanguage.db"
    limit = None
    db_batch_size = 100000

    try:
        os.remove(db_path)
    except OSError:
        pass
    with WikilanguageDB.con(db_path) as db:
        db.create_tables()
        for path in wiki_paths:
            wikiname = os.path.basename(path).split("-")[0]
            print(f"Main: starting write {wikiname}")
            in_memory = wikiname != "enwiki"
            pipelines.write_articles_into_db(
                db,
                path,
                wikiname,
                rank_in_memory=in_memory,
                limit=limit,
                db_batch_size=db_batch_size,
            )

        print(f"Main: done wiki writes, loading inheritance graph")
        parent_finder = WikiDataInheritanceGraph.load(inheritance_path).parent_finder()
        print(f"Main: writing wikidata")
        pipelines.write_wikidata_into_db(
            db, wikidata_path, parent_finder, limit=limit, db_batch_size=db_batch_size
        )


if __name__ == "__main__":
    main()
