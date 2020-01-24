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
    wikidata_path = "data/20200101/wikidata-20200113-all.json.gz"
    wiki_paths = glob.glob("data/20200101/*-pages-articles*")
    wiki_paths.sort(key=lambda p: os.stat(p).st_size, reverse=True)

    limit = None
    output_path = "data/wikilanguage.tsv"
    whitelisted_wikis = None
    working_dir = "working-dir/"

    # limit = 10000
    # output_path = "data/test.tsv"
    # whitelisted_wikis = {"enwiki", "jawiki"}
    # working_dir = "working-test/"

    if not os.path.exists(wikidata_path):
        raise RuntimeError(f"{wikidata_path} not found!")

    wiki_shelves = {}
    temps = []
    try:
        for path in wiki_paths:
            wikiname = os.path.basename(path).split("-")[0]
            if whitelisted_wikis and wikiname not in whitelisted_wikis:
                continue

            shelf_working_path = (
                os.path.join(working_dir, f"{wikiname}") if working_dir else None
            )

            if shelf_working_path and glob.glob(f"{shelf_working_path}*"):
                print(f"Main: Reading shelf from {shelf_working_path}")
                shelf = shelve.open(shelf_working_path)
            else:
                temp = tempfile.NamedTemporaryFile(delete=False)
                temp.close()
                os.remove(temp.name)
                in_memory = wikiname != "enwiki"
                print(f"Main: starting write {wikiname} to {temp.name} (in_memory={in_memory})")
                temps.append(temp)
                shelf = shelve.open(temp.name)
                pipelines.write_articles_to_shelf(
                    shelf, path, rank_in_memory=in_memory, limit=limit
                )
                if shelf_working_path:
                    for desc in glob.glob(f"{temp.name}*"):
                        _, ext = os.path.splitext(desc)
                        print(
                            f"Main: copying shelf {desc} to {shelf_working_path}{ext}"
                        )
                        os.rename(f"{desc}", f"{shelf_working_path}{ext}")
                    shelf = shelve.open(shelf_working_path)

            wiki_shelves[wikiname] = shelf

        print(f"Main: done wiki writes, loading inheritance graph")

        inheritance_working_path = (
            os.path.join(working_dir, f"inheritance.pickle") if working_dir else None
        )
        if inheritance_working_path and os.path.exists(inheritance_working_path):
            print(f"Main: loading inheritance graph from {inheritance_working_path}")
            inheritance_graph = WikiDataInheritanceGraph.load(inheritance_working_path)
        else:
            inheritance_graph = pipelines.wikidata_inheritance_graph(
                wikidata_path, limit=limit
            )
            print(f"Main: Loaded inheritance graph!")
            if inheritance_working_path:
                print(f"Main: dumping to {inheritance_working_path}")
                inheritance_graph.dump(inheritance_working_path)

        parent_finder = inheritance_graph.parent_finder()
        del inheritance_graph
        print(f"Main: writing wikidata to {output_path}")
        pipelines.write_csv(
            wikidata_path,
            output_path,
            wiki_shelves,
            parent_finder,
            limit=limit,
            whitelisted_wikis=whitelisted_wikis,
        )
        print(f"Done write to {output_path}!")

    finally:
        for shelf in wiki_shelves.values():
            shelf.close()

        for temp in temps:
            for desc in glob.glob(f"{temp.name}*"):
                try:
                    os.remove(desc)
                except FileNotFoundError:
                    pass


if __name__ == "__main__":
    main()
