import itertools
import tempfile
import pipelines
import time
import os
import glob
from collections import defaultdict, OrderedDict
from wikidata_parser import WikiDataParser, WikiDataInheritanceGraph
import shelve
import tempfile


def main():
    wikidata_path = "wikis-2019-11-17/latest-all.json.bz2"
    wiki_paths = glob.glob("wikis-2019-11-17/*-pages-articles*")
    # inheritance_path = "data/wikidata_inheritance.pickle"
    inheritance_path = "data/test.pickle"
    output_path = "data/test.tsv"
    limit = 1000000
    # whitelisted_wikis = None
    whitelisted_wikis = {"enwiki", "jawiki"}

    wiki_shelves = {}
    temps = []
    try:
        for path in wiki_paths:
            wikiname = os.path.basename(path).split("-")[0]
            if whitelisted_wikis and wikiname not in whitelisted_wikis:
                continue

            temp = tempfile.NamedTemporaryFile(delete=False, suffix=".dat")
            temp.close()
            print(f"Main: starting write {wikiname} to {temp.name}")
            temps.append(temp)
            shelf = shelve.open(temp.name, flag="n")
            wiki_shelves[wikiname] = shelf
            in_memory = wikiname != "enwiki"
            pipelines.write_articles_to_shelf(
                shelf, path, rank_in_memory=in_memory, limit=limit
            )

        print(f"Main: done wiki writes, loading inheritance graph")
        inheritance_graph = pipelines.wikidata_inheritance_graph(
            wikidata_path, limit=limit
        )
        print(f"Loaded! Dumping to {inheritance_path}")
        inheritance_graph.dump(inheritance_path)
        parent_finder = inheritance_graph.parent_finder()
        inheritance_graph = None
        print(f"Main: writing wikidata")
        pipelines.write_csv(
            wikidata_path,
            output_path,
            wiki_shelves,
            parent_finder,
            limit=limit,
            whitelisted_wikis=whitelisted_wikis,
        )

    finally:
        for shelf in wiki_shelves.values():
            shelf.close()

        for temp in temps:
            os.remove(temp.name)


if __name__ == "__main__":
    main()
